package com.google.appengine.tools.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.appengine.repackaged.com.google.common.collect.Maps;

/**
 * A {@code RecordWriter} that writes the intermediate results of the mapping stage to the
 * datastore, merging values by key as they are written.
 *
 * See {@link com.google.appengine.tools.mapreduce.KeyedValueList}
 */
public class IntermediateWriter extends RecordWriter<OutputKey, Writable> {
  private final AppEngineTaskAttemptContext context;
  private final DatastoreService datastoreService;

  private static int DEFAULT_CACHE_SIZE = 1 << 17;
  
  private OutputKeyRange keyRange;
  private BuilderCache cache;
  private AppEngineReducer combiner;
  private Reducer.Context combinerContext;

  public IntermediateWriter(DatastoreService datastoreService, AppEngineTaskAttemptContext context) {
    this(datastoreService, context, DEFAULT_CACHE_SIZE);
    
    Class combinerClass;
    try {
      combinerClass = context.getCombinerClass();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Combiner class not found", e);
    }
    if(combinerClass != null) {
      combiner = (AppEngineReducer)ReflectionUtils.newInstance(combinerClass, context.getConfiguration());
    }
  }

  public IntermediateWriter(DatastoreService datastoreService, AppEngineTaskAttemptContext context, int cacheSize) {
    this.context = context;
    this.datastoreService = datastoreService;

    keyRange = context.getShardState().getOutputKeyRange();
    cache = new BuilderCache(cacheSize);
  }

  
  public void write(OutputKey key, Writable value) throws IOException {
    updateRange(key);
    try {
      writeValue(key, value);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }
  }

  private void updateRange(OutputKey key) {
    if(keyRange.addKey(key.getKeyString())) {
      context.getShardState().setOutputKeyRange(keyRange);
    }
  }

  private void writeValue(OutputKey key, Writable value) throws IOException, InterruptedException {
    KeyedValueListBuilder listShard = getListShard(key);
    int bytesWritten = listShard.appendValue(value);
    
    persist(cache.flush(bytesWritten));
  }

  private KeyedValueListBuilder getListShard(OutputKey key) throws IOException {

    KeyedValueListBuilder listShard = cache.get(key.getKeyString());
    if(listShard == null) {
      listShard =	KeyedValueListBuilder.restoreOrCreate(datastoreService,
          context.getTaskAttemptID(), key);
      
      cache.cache(key.getKeyString(), listShard);
    }
    return listShard;
  }

  public void flush() throws IOException {
    persist(cache.flush());
  }

  private void persist(Collection<KeyedValueListBuilder> builders) {
    List<Entity> shards = new ArrayList<Entity>();
    for(KeyedValueListBuilder builder : combine(builders)) {
      shards.add(builder.toEntity());
    }
    if(!shards.isEmpty()) {
      datastoreService.put(shards);
    }
  }
  

  private Collection<KeyedValueListBuilder> combine(Collection<KeyedValueListBuilder> input) {
    if(combiner == null) {
      return input;
    }
   
    try {
      CombinedWriter output = new CombinedWriter();
      combinerContext = AppEngineReducer.getReducerContext(
          combiner, 
          context,
          output, 
          new NullOutputCommitter(), 
          context.getConfiguration(), 
          context.getTaskAttemptID());
      
      combiner.setup(combinerContext);
    
      // create an instance of our value class
      Writable valueInstance = (Writable) ReflectionUtils.newInstance(context.getMapOutputValueClass(), 
          context.getConfiguration());
      
      for(KeyedValueListBuilder builder : input) {
        combiner.reduce(builder.getOutputKey(), 
            new ShardedRawValueIterable(
                new RawValueList(Collections.singletonList(builder.getBlob())), valueInstance), combinerContext);
        
      }   
      
      return output.getBuilders();
      
    } catch(InterruptedException e) {
      throw new RuntimeException("InterruptedException thrown while combining", e);
    } catch (IOException e) {
      throw new RuntimeException("IOException thrown while combining", e);
    }
  }
  

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    /* NO OP */
  }


  /**
   * LRU cache of list builders. We keep lists in memory as long as possible before
   * writing them to the datastore to make room for lists for other keys.
   * 
   * @author alex@bedatadriven.com
   *
   */
  static class BuilderCache {
    private Map<String, KeyedValueListBuilder> map = new HashMap<String, KeyedValueListBuilder>();
    private LinkedList<String> leastRecent = new LinkedList<String>();

    private int size = 0;
    private int maxSize;

    public BuilderCache(int maxSize) {
      this.maxSize = maxSize;
    }

    public KeyedValueListBuilder get(String key) {
      KeyedValueListBuilder builder = map.get(key);
      if(builder != null) {
        if(!leastRecent.getLast().equals(key)) {
          leastRecent.remove(key);
          leastRecent.add(key);
        }
      }
      return builder;
    } 

    public void cache(String key, KeyedValueListBuilder builder) {
      map.put(key, builder);
      leastRecent.add(key);
    }

    public Collection<KeyedValueListBuilder> flush(int bytesWritten) {
      size += bytesWritten;
      List<KeyedValueListBuilder> entities = new ArrayList<KeyedValueListBuilder>();
      while(size > maxSize) {
        String keyToEvict = leastRecent.pop();
        KeyedValueListBuilder evictedBuilder = map.remove(keyToEvict);

        entities.add(evictedBuilder);
        size -= evictedBuilder.size();
      }
      
      return entities;
    }
    
    public Collection<KeyedValueListBuilder> flush() {
      List<KeyedValueListBuilder> values = Lists.newArrayList(map.values());
      map.clear();
      leastRecent.clear();
      
      return values;
    }

    public Iterable<KeyedValueListBuilder> values() {
      return map.values();
    }
  }
  
  private class CombinedWriter extends RecordWriter<OutputKey, Writable> {

    private TaskAttemptID attemptID;
    private Map<String, KeyedValueListBuilder> builders = Maps.newHashMap();
    
    @Override
    public void write(OutputKey key, Writable value) throws IOException,
        InterruptedException {
      
      String keyString = key.getKeyString();
      KeyedValueListBuilder builder = builders.get(keyString);
      
      if(builder == null) {
        builder = new KeyedValueListBuilder(key, context.getTaskAttemptID());
        builders.put(keyString, builder);
      }
      builder.appendValue(value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {

    }
    
    public Collection<KeyedValueListBuilder> getBuilders() {
      return builders.values();
    }
  }
}

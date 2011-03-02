package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

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
  
  private static final Logger logger = Logger.getLogger(IntermediateWriter.class.getName());


  public IntermediateWriter(DatastoreService datastoreService, AppEngineTaskAttemptContext context) {
    this(datastoreService, context, DEFAULT_CACHE_SIZE);
  }

  public IntermediateWriter(DatastoreService datastoreService, AppEngineTaskAttemptContext context, int cacheSize) {
    this.context = context;
    this.datastoreService = datastoreService;

    keyRange = context.getShardState().getOutputKeyRange();
    cache = new BuilderCache(cacheSize);
    
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
  
  @VisibleForTesting
  BuilderCache getBuilderCache() {
    return cache;
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
    persist(combine(cache.flush()));
  }

  private void persist(Collection<KeyedValueListBuilder> builders) {
    List<Entity> shards = new ArrayList<Entity>();
    for(KeyedValueListBuilder builder : builders) {
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
  class BuilderCache {
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
      
      // if we've exceeded our cache limit, and we have a combiner,
      // do a first combining pass to reduce our memory footprint
      if(size > maxSize && combiner != null) {
        int newSize = 0;
        Collection<KeyedValueListBuilder> combinedBuilders = combine(map.values()); 
        for(KeyedValueListBuilder combined : combinedBuilders) {
          newSize += combined.size();
          map.put(combined.getOutputKey().getKeyString(), combined);
        }
        size = newSize;
      }

      List<KeyedValueListBuilder> entitiesToWrite = new ArrayList<KeyedValueListBuilder>();

      if(size > maxSize) {

        // we want to avoid the situation where the cache fills up and
        // every time we write a new value we have to evict another single key/value
        // rather, we'd prefer to evict a bunch old entries in one go.

        // there's probably a elegant adaptive algorithm, but the
        // following will do till it gets here.

        while(size > (maxSize/2)) {
          String keyToEvict = leastRecent.pop();
          KeyedValueListBuilder evictedBuilder = map.remove(keyToEvict);

          entitiesToWrite.add(evictedBuilder);
          size -= evictedBuilder.size();
        }

        if(!entitiesToWrite.isEmpty()) {
          logger.info("Evicting " + entitiesToWrite.size() + " entities from builder cache");
        }
      }
      
      
      return entitiesToWrite;
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

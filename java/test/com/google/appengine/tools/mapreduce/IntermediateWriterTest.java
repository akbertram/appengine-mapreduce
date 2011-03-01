package com.google.appengine.tools.mapreduce;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.easymock.Capture;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.common.collect.Lists;

public class IntermediateWriterTest extends TestCase {
  
  private static final int SIZE_OF_LONG = 8;
  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig());

  private TaskAttemptID taskAttemptID;
  
  public void setUp() {
    helper.setUp();
  }

  public void tearDown() {
    helper.tearDown();
  }
  
  public void testBuilderCache() { 
    IntermediateWriter.BuilderCache cache = new IntermediateWriter.BuilderCache(256);
    KeyedValueListBuilder builder = new KeyedValueListBuilder(outputKey("A"), taskAttempt(1));
    
    assertNull(cache.get("A"));
    
    cache.cache("A", builder);
    
    assertSame(builder, cache.get("A"));
  }

  public void testCacheEviction() throws IOException {
    IntermediateWriter.BuilderCache cache = new IntermediateWriter.BuilderCache(8);
   
    // add the first item to the list
    // we will still be under max size, so no evictions should occur
    
    KeyedValueListBuilder list1 = new KeyedValueListBuilder(outputKey("A"), taskAttempt(1));
    cache.cache("A", list1);
    
    int bytesWritten = list1.appendValue(new LongWritable(1));
    
    assertTrue(cache.flush(bytesWritten).isEmpty());
    
    // add the next item to the list
    // after writing, we will be over max, so list 1 should be evicted as it was
    // the least recently used
    
    KeyedValueListBuilder list2 = new KeyedValueListBuilder(outputKey("A"), taskAttempt(1));
    cache.cache("B", list2);
    
    bytesWritten = list2.appendValue(new LongWritable(2));
    
    Collection<KeyedValueListBuilder> evicted = cache.flush(bytesWritten);
    assertEquals(1, evicted.size());
    assertSame(list1, evicted.iterator().next());
  }
  
  public void testLRU() throws IOException {

    IntermediateWriter.BuilderCache cache = new IntermediateWriter.BuilderCache(16);

    // add the first item to the list
    // we will still be under max size, so no evictions should occur
    
    KeyedValueListBuilder list1 = new KeyedValueListBuilder(outputKey("A"), taskAttempt(1));
    cache.cache("A", list1);
    
    int bytesWritten = list1.appendValue(new LongWritable(1));
    
    assertTrue(cache.flush(bytesWritten).isEmpty());
    
    // add the next item to the list
    
    KeyedValueListBuilder list2 = new KeyedValueListBuilder(outputKey("A"), taskAttempt(1));
    cache.cache("B", list2);
    
    bytesWritten = list2.appendValue(new LongWritable(2));
    
    assertTrue(cache.flush(bytesWritten).isEmpty());

    // now access the list1
    
    assertSame(list1, cache.get("A"));
    bytesWritten = list1.appendValue(new LongWritable(3));
    
    Collection<KeyedValueListBuilder> evicted = cache.flush(bytesWritten);
    assertEquals(1, evicted.size());
    assertSame(list2, evicted.iterator().next()); 
  }
  
  public void testWriter() throws IOException {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    
    ShardState shard = createMock(ShardState.class);
    expect(shard.getOutputKeyRange()).andReturn(new OutputKeyRange());
    shard.setOutputKeyRange(isA(OutputKeyRange.class));
    expectLastCall().anyTimes();
    replay(shard);
    
    AppEngineTaskAttemptContext context = createMock(AppEngineTaskAttemptContext.class);
    expect(context.getShardState()).andReturn(shard).anyTimes();
    expect(context.getTaskAttemptID()).andReturn(new TaskAttemptID()).anyTimes();
    replay(context);
    
    IntermediateWriter writer = new IntermediateWriter(
        datastore,
        context, 
        16);
    
    writer.write(new LongOutputKey(1), new LongWritable(1));
    writer.write(new LongOutputKey(2), new LongWritable(1));
    writer.write(new LongOutputKey(2), new LongWritable(1));
    writer.write(new LongOutputKey(1), new LongWritable(1));
    writer.write(new LongOutputKey(3), new LongWritable(1));
    
    writer.flush();

    Query query = new Query(KeyedValueList.KIND);
    
    List lists = datastore.prepare(query).asList(FetchOptions.Builder.withDefaults());
    
    assertEquals(3, lists.size());
   
  }

  

  public void testCombiner() throws Exception {
    Capture<Iterable<Entity>> results = new Capture<Iterable<Entity>>();
    DatastoreService datastore = createMock(DatastoreService.class);
    expect(datastore.get(isA(Key.class))).andThrow(new EntityNotFoundException(null)).anyTimes();
    expect(datastore.put(capture(results))).andReturn(null);
    replay(datastore);
    
    ShardState shard = createMock(ShardState.class);
    expect(shard.getOutputKeyRange()).andReturn(new OutputKeyRange());
    shard.setOutputKeyRange(isA(OutputKeyRange.class));
    expectLastCall().anyTimes();
    replay(shard);
    
    Configuration config = new Configuration();
    
    AppEngineTaskAttemptContext context = createMock("Context", AppEngineTaskAttemptContext.class);
    expect(context.getShardState()).andReturn(shard).anyTimes();
    expect(context.getTaskAttemptID()).andReturn(new TaskAttemptID()).anyTimes();
    expect(context.getCombinerClass()).andReturn((Class)SummingCombiner.class);
    expect(context.getMapOutputValueClass()).andReturn((Class)LongWritable.class).anyTimes();
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    replay(context);
    
    IntermediateWriter writer = new IntermediateWriter(
        datastore,
        context);
    
    writer.write(outputKey("A"), new LongWritable(71));
    writer.write(outputKey("B"), new LongWritable(3));
    writer.write(outputKey("A"), new LongWritable(13));
    writer.write(outputKey("B"), new LongWritable(11));
    writer.write(outputKey("C"), new LongWritable(23));

    writer.flush();

    verify(datastore, shard, context);
    
    List<Entity> combinedValues = Lists.newArrayList(results.getValue());
    assertEquals(3, combinedValues.size());
    assertSingleResultWithValue(combinedValues, "A", 84);
    assertSingleResultWithValue(combinedValues, "B", 14);
    assertSingleResultWithValue(combinedValues, "C", 23);
  }
  
  private void assertSingleResultWithValue(Iterable<Entity> entities, String name, long value) throws IOException {
    for(Entity entity : entities) {
      if(KeyedValueList.parseRawKey(entity.getKey()).equals(name)) {
        Blob serializedValue = (Blob) entity.getProperty(KeyedValueList.LIST_PROPERTY);
        assertEquals("blob length of " + name, SIZE_OF_LONG, serializedValue.getBytes().length);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serializedValue.getBytes()));
        assertEquals(name, value, dis.readLong());
        return;
      }
    }
    fail("No entity with key name of '"  + name + "'");
  }
  
  
  private StringOutputKey outputKey(String s) {
    return new StringOutputKey(s);
  }
  
  private TaskAttemptID taskAttempt(int i) {
    return new TaskAttemptID("201001010", 1, true, 99, i);
  }
  
  private static class StringOutputKey extends OutputKey {
    private String value;
    
    public StringOutputKey(String value) {
      super();
      this.value = value;
    }

    @Override
    public String getKeyString() {
      return value;
    }

    @Override
    public void readFromKeyString(String keyString) {
      this.value = keyString;
    }
  }
  
  public static class SummingCombiner extends AppEngineReducer<StringOutputKey, LongWritable, 
                                                                StringOutputKey, LongWritable> {

    @Override
    public void reduce(StringOutputKey key, Iterable<LongWritable> values,
        Context context) throws IOException, InterruptedException {
      
      long sum = 0;
      for(LongWritable value : values) {
        sum += value.get();
      }
      
      context.write(key, new LongWritable(sum));
    }
  }
}

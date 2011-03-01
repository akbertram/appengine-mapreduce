package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Blob;
import com.google.common.collect.Iterators;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.easymock.classextension.EasyMock.*;

/**
 * Unit test for {@code ReducingMapperTest}
 *
 * @author alex@bedatadriven.com (Alex Bertram)
 */
public class ReducingMapperTest extends TestCase {
  private Mapper.Context context;

  public ReducingMapperTest() throws ClassNotFoundException {
    context = mockContext();
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  public void testContextCreation() throws IOException, InterruptedException {
    ReducingMapper mapper = new ReducingMapper();
    mapper.taskSetup(context);
  }

  public void testIterator() throws IOException, InterruptedException {
    ReducingMapper mapper = new ReducingMapper();
    mapper.taskSetup(context);

    Iterable<IntWritable> valueList = mapper.createValueIterable(
        new RawValueList(Arrays.asList(
            blob(1,2,3),
            blob(4),
            blob(5, 6, 7, 8),
            blob(9)))
    );

    assertEquals(9, Iterators.size(valueList.iterator()));

    int expected = 1;
    Iterator<IntWritable> it = valueList.iterator();

    while(it.hasNext()) {
      assertEquals(expected++, it.next().get());
    }
  }

  public void testReduce() throws IOException, InterruptedException {
    ReducingMapper mapper = new ReducingMapper();
    mapper.taskSetup(context);

    String encodedKey = new LongOutputKey(41).getKeyString();
    RawValueList valueList = new RawValueList(Arrays.asList(blob(1, 2, 3, 4)));

    mapper.map(encodedKey, valueList, context);
  }


  private Mapper.Context mockContext() throws ClassNotFoundException {
    Configuration conf = new Configuration();

    AppEngineMapper.Context context = createMock(AppEngineMapper.Context.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(context.getTaskAttemptID()).andReturn(new TaskAttemptID("20101004", 1, true, 1, 0)).anyTimes();
    expect(context.getReducerClass()).andReturn((Class) ReducerStub.class).anyTimes();
    expect(context.getCounter(isA(String.class), isA(String.class))).andReturn(null).anyTimes();
    expect(context.getMapOutputKeyClass()).andReturn((Class) LongOutputKey.class).anyTimes();
    expect(context.getMapOutputValueClass()).andReturn((Class) IntWritable.class).anyTimes();
    replay(context);
    return context;
  }

  private Blob blob(int... values) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for(int d : values) {
      baos.write(Writables.createByteArrayFromWritable(new IntWritable(d)));
    }
    return new Blob(baos.toByteArray());
  }

  public static class ReducerStub extends AppEngineReducer<OutputKey, IntWritable,
      LongWritable, IntWritable> {

    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      if(key.get() != 41) {
        throw new AssertionError("Expected key with value of 41, got " + key.get());
      }
    }
  }

}

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;

/**
 * Unit test for {@code IntermediateRecordReader}
 *
 * @author alex@bedatadriven.com (Alex Bertram)
 */
public class IntermediateRecordReaderTest extends TestCase {

  public static final String JOB_TRACKER_ID = "1285368634691";
  public static final JobID MAPPER_JOB_ID = new JobID(JOB_TRACKER_ID, 1);

  public static final int SHARD1 = 1;
  public static final int SHARD2 = 2;
  public static final int SHARD3 = 3;

  private static TaskAttemptID attemptId(int taskId) {
    return new TaskAttemptID(JOB_TRACKER_ID, SHARD1, true, taskId, 0);
  }

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig());


  private DatastoreService datastore;


  public void setUp() {
    helper.setUp();
    datastore = DatastoreServiceFactory.getDatastoreService();
  }

  public void tearDown() {
    helper.tearDown();
  }

  public void testPersistance() throws IOException, InterruptedException {
    // fill the datastore with test data
    emitKeyValues(SHARD1, "A", 1, 2, 3);
    emitKeyValues(SHARD1, "B", 1, 2, 3);
    emitKeyValues(SHARD1, "C", 1, 2, 3);

    emitKeyValues(SHARD2, "A", 4);
    emitKeyValues(SHARD2, "C", 4);

    emitKeyValues(SHARD3, "B", 4, 5, 6);
    emitKeyValues(SHARD3, "C", 5, 6);

    // define a single split
    InputSplit inputSplit = KeyedValueList.createSplit(MAPPER_JOB_ID, "A", "Z");

    // now start up the input Reader
    IntermediateRecordReader reader = new IntermediateRecordReader();
    reader.initialize(inputSplit, null);

    reader.nextKeyValue();
    assertEquals("A", reader.getCurrentKey());
    assertEquals(4, countValues(reader));

    // stop, persist, restore, and restart
    byte[] state = Writables.createByteArrayFromWritable(reader);

    reader = new IntermediateRecordReader();
    reader.initialize(inputSplit, null);
    Writables.initializeWritableFromByteArray(state, reader);

    reader.nextKeyValue();
    assertEquals("B", reader.getCurrentKey());
    assertEquals(6, countValues(reader));


  }

  private int countValues(IntermediateRecordReader reader) throws IOException, InterruptedException {
    int count = 0;
    for(Blob blob : reader.getCurrentValue().getBlobs()) {
      count += blob.getBytes().length / 4;
    }
    return count;
  }

  private void emitKeyValues(int shard, String key, int... values) throws IOException {
    KeyedValueListBuilder builder = KeyedValueListBuilder.restoreOrCreate(datastore, attemptId(shard),
        new TextOutputKey(key));
    for(int i : values) {
      builder.appendValue(new IntWritable(i));
    }
    datastore.put(builder.toEntity());
  }


}

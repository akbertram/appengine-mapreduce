package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import junit.framework.TestCase;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;

/**
 * Unit test for {@code KeyedValueListShard}
 *
 * @author alex@bedatadriven.com (Alex Bertram)
 */
public class KeyedValueListShardTest extends TestCase {

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig());


  public void setUp() {
    helper.setUp();
  }

  public void tearDown() {
    helper.tearDown();
  }


  public void testPersist() throws IOException {
    TextOutputKey key = new TextOutputKey("49");
    Writable value = new DoubleWritable(3.143);

    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    TaskAttemptID attemptId = new TaskAttemptID("Foo", 1, true, 1, 0);

    KeyedValueListBuilder shard = KeyedValueListBuilder.restoreOrCreate(datastore, attemptId, key);

    shard.appendValue(new DoubleWritable(3.143));
    shard.appendValue(new DoubleWritable(40234.324));

    datastore.put(shard.toEntity());
  }

}

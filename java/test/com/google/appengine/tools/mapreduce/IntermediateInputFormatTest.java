package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import junit.framework.TestCase;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;

import java.util.List;

/**
 * Unit test for {@code IntermediateInputFormat}
 *
 * @author alex@bedatadriven.com (Alex Bertram)
 */
public class IntermediateInputFormatTest extends TestCase {

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig());
  public static final JobID MAPPER_JOB_ID = JobID.forName("job_1285368634691_0001");


  public void setUp() {
    helper.setUp();
  }

  public void tearDown() {
    helper.tearDown();
  }

  public void testSplits() {

    IntermediateInputFormat inputFormat = new IntermediateInputFormat();

    List<InputSplit> splits = inputFormat.getSplits(MAPPER_JOB_ID, "A", "K", 4);

    assertEquals(4, splits.size());
  }

  public void testSingleSplit() {

    IntermediateInputFormat inputFormat = new IntermediateInputFormat();

    List<InputSplit> splits = inputFormat.getSplits(
        MAPPER_JOB_ID, "A", "Z", 1);
    
    DatastoreInputSplit split = (DatastoreInputSplit) splits.get(0);
    assertEquals(1, splits.size());

    // verify that the range includes the maximum key
    String keyName = KeyedValueList.keyName(MAPPER_JOB_ID, "Z", 9999);
    assertTrue( split.getStartKey().getName().compareTo(keyName) <= 0 );
    assertTrue( keyName.compareTo( split.getEndKey().getName() ) <  0 );
    
  }

  private OutputKeyRange range(String min, String max) {
    OutputKeyRange range = new OutputKeyRange();
    range.addKey(min);
    range.addKey(max);
    return range;
  }

}

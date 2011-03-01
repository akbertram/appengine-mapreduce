package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import junit.framework.TestCase;

/**
 * Unit test for the {@code ShardKeyRange} class
 *
 * @author alex@bedatadriven.com (Alex Bertram)
 */
public class ShardKeyRangeTest extends TestCase {


  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig());


  public void setUp() {
    helper.setUp();
  }

  public void tearDown() {
    helper.tearDown();
  }

  public void testFirstAdd() {

    OutputKeyRange range = new OutputKeyRange();
    range.addKey("X");

    assertEquals("minKey", range.getMinKey(), "X");
    assertEquals("maxKey", range.getMaxKey(), "X");
  }


  public void testSeq() {

    OutputKeyRange range = new OutputKeyRange();
    range.addKey("P");
    range.addKey("Z");
    range.addKey("H");
    range.addKey("C");
    range.addKey("A");

    assertEquals("minKey", range.getMinKey(), "A");
    assertEquals("maxKey", range.getMaxKey(), "Z");
  }
}

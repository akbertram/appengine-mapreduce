/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.fs.AppEngineFileSystemTest;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Suite of all tests.
 *
 * @author frew@google.com (Fred Wulff)
 */
public class AllTests extends TestCase {

  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTestSuite(AppEngineJobContextTest.class);
    suite.addTestSuite(AppEngineMapperTest.class);
    suite.addTestSuite(ConfigurationTemplatePreprocessorTest.class);
    suite.addTestSuite(DatastoreInputFormatTest.class);
    suite.addTestSuite(DatastoreMutationPoolTest.class);
    suite.addTestSuite(DatastoreRecordReaderTest.class);
    suite.addTestSuite(DatastoreSerializationUtilTest.class);
    suite.addTestSuite(MapReduceServletTest.class);
    suite.addTestSuite(MapReduceXmlTest.class);
    suite.addTestSuite(QuotaConsumerTest.class);
    suite.addTestSuite(QuotaManagerTest.class);
    suite.addTestSuite(StringSplitUtilTest.class);
    suite.addTestSuite(BlobstoreInputFormatTest.class);
    suite.addTestSuite(BlobstoreInputSplitTest.class);
    suite.addTestSuite(BlobstoreRecordReaderTest.class);
    suite.addTestSuite(InputStreamIteratorTest.class);
    suite.addTestSuite(RangeInputFormatTest.class);
    suite.addTestSuite(RangeRecordReaderTest.class);

    // AppengineFS
    suite.addTestSuite(AppEngineFileSystemTest.class);

    // Reducer stuff
    suite.addTestSuite(IntermediateInputFormatTest.class);
    suite.addTestSuite(IntermediateRecordReaderTest.class);
    suite.addTestSuite(KeyedValueListShardTest.class);
    suite.addTestSuite(ReducingMapperTest.class);
    suite.addTestSuite(ShardKeyRangeTest.class);
    suite.addTestSuite(LongOutputKeyTest.class);
    suite.addTestSuite(WritableOutputKeyTest.class);
    suite.addTestSuite(OutputKeyRangeTest.class);
    
    return suite;
  }
}

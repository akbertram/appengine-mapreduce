package com.google.appengine.tools.mapreduce;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.AppEngineMapper.AppEngineContext;

import junit.framework.TestCase;

public class OutputKeyRangeTest extends TestCase {

	private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
			new LocalDatastoreServiceTestConfig());

	private DatastoreService datastoreService;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		helper.setUp();
		datastoreService = DatastoreServiceFactory.getDatastoreService();

	}

	@Override
	protected void tearDown() throws Exception {
		helper.tearDown();
		super.tearDown();
	}

	public void testAggregationWithEmptyShards() {
		
		TaskID taskId = new TaskID("JT1", 1, true, 1);
		
		ShardState shard1 = ShardState.generateInitializedShardState(datastoreService, 
				new TaskAttemptID(taskId, 1));
		shard1.setOutputKeyRange(new OutputKeyRange("A", "K"));
		
		ShardState shard2 = ShardState.generateInitializedShardState(datastoreService, 
				new TaskAttemptID(taskId, 2));
		
		
		ShardState shard3 = ShardState.generateInitializedShardState(datastoreService, 
				new TaskAttemptID(taskId, 3));
		shard3.setOutputKeyRange(new OutputKeyRange("N", "N"));

		OutputKeyRange agg = OutputKeyRange.aggregate(Arrays.asList(shard1, shard2, shard3));
		
		assertEquals("A", agg.getMinKey());
		assertEquals("N", agg.getMaxKey());
	
		
		
	}

}

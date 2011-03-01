package com.google.appengine.tools.mapreduce;

import org.apache.hadoop.io.LongWritable;

import junit.framework.TestCase;

public class WritableOutputKeyTest extends TestCase {
	
	public void testPersistance() {
		
		WritableOutputKey x = new WritableOutputKey(new LongWritable(41));
		String keyString = x.getKeyString();
		
		LongWritable y = new LongWritable();
		WritableOutputKey yAdapter = new WritableOutputKey(y);
		
		yAdapter.readFromKeyString(keyString);
		
		assertEquals(41, y.get());
		
		
	}
	

}

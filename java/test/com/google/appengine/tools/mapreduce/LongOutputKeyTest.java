package com.google.appengine.tools.mapreduce;

import junit.framework.TestCase;

public class LongOutputKeyTest extends TestCase {
	
	
	public void testLength() {
		assertCorrectLength(0);
		assertCorrectLength(-1);
		assertCorrectLength(-256);
		assertCorrectLength(256);
		assertCorrectLength(Long.MAX_VALUE);
		assertCorrectLength(Long.MIN_VALUE);
	}

	private void assertCorrectLength(long value) {
		assertEquals(Long.toString(value), 14, new LongOutputKey(value).getKeyString().length());
	}
	
	public void testPersist() {
		
		LongOutputKey key = new LongOutputKey(Long.MAX_VALUE);
		String keyString = key.getKeyString();
		
		LongOutputKey rekey = new LongOutputKey();
		rekey.readFromKeyString(keyString);
		
		assertEquals(key.get(), rekey.get());
		
		
	}

}

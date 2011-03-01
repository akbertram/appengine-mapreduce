package com.google.appengine.tools.mapreduce;

public class LongOutputKey extends OutputKey {
	private static final char[] PADDING = new char[] { 
		'0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0' };
	
	private static final int RADIX = 36;
	private static final int MAX_DIGITS = 13;
	
	private long value;

	
	public LongOutputKey() {
		
	}
	
	public LongOutputKey(long value) {
		this.value = value;
	}
	
	public long get() {
		return value;
	}
	
	public void set(long value) {
		this.value = value;
	}
	
	@Override
	public String getKeyString() {
		
		// random special case: for Long.MIN_VALUE there is no equivalent postive value!
		if( value == Long.MIN_VALUE) {
			return Long.toString(value, RADIX);
		}
		
		StringBuffer s = new StringBuffer();
		s.append( Long.signum(value) == -1 ? '-' : '+');
		
		String num = Long.toString(Math.abs(value), RADIX);  
		
		s.append(PADDING, 0, MAX_DIGITS - num.length());
		s.append(num);
		
		return s.toString();
	}

	@Override
	public void readFromKeyString(String keyString) {
		
		value = Long.parseLong(keyString.substring(1), RADIX);
		
		if(keyString.charAt(0) == '-') {
			value = -value;
		}
		
	}
}

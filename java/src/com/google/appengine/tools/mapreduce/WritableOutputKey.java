package com.google.appengine.tools.mapreduce;

import org.apache.hadoop.io.Writable;

/**
 * An {@code OutputKey} adapter for {@code Writable}s that provides 
 * keyStrings as a hex encoding
 * of the {@code Writable}'s byte stream. 
 * 
 * The resulting sort order of the keys is undefined.
 * 
 * @author alex@bedatadriven.com (Alex Bertram)
 * 
 */
public class WritableOutputKey extends OutputKey {

	private Writable writable;
	private static final String DIGITS = "0123456789ABCDEF";

	public WritableOutputKey(Writable w) {
		this.writable = w;
	}

	public Writable get() {
		return writable;
	}

	public void set(Writable w) {
		this.writable = w;
	}

	@Override
	public String getKeyString() {
		byte bytes[] = Writables.createByteArrayFromWritable(writable);
		StringBuffer buffer = new StringBuffer(bytes.length * 2);
		for (byte b : bytes) {
			buffer.append(DIGITS.charAt((b & 0xF0) >> 4));
			buffer.append(DIGITS.charAt((b & 0x0F)));
		}
		return buffer.toString();
	}

	@Override
	public void readFromKeyString(String keyString) {
		byte bytes[] = new byte[keyString.length() / 2];
		for (int i = 0; i != bytes.length; i++) {
			int high = DIGITS.indexOf(keyString.charAt(i * 2));
			int low = DIGITS.indexOf(keyString.charAt((i * 2) + 1));

			bytes[i] = (byte) ((high << 4) | low);
		}
		Writables.initializeWritableFromByteArray(bytes, writable);
	}
}

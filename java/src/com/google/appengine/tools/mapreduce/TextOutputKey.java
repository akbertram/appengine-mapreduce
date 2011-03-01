package com.google.appengine.tools.mapreduce;

/**
 * An {@code OutputKey} for Strings. 
 * 
 * @author alex@bedatadriven.com (Alex Bertram)
 */
public class TextOutputKey extends OutputKey {

	private String text;
	
	public TextOutputKey() {
		
	}
	
	public TextOutputKey(String text) {
		super();
		this.text = text;
	}

	public String get() {
		return text;
	}
	
	public void set(String text) {
		this.text = text;
	}
	
	@Override
	public String getKeyString() {
		return text;
	}

	@Override
	public void readFromKeyString(String keyString) {
		this.text = keyString;
		
	}

	
	
}

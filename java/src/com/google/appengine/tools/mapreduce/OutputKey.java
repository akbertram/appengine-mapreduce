package com.google.appengine.tools.mapreduce;

import java.util.Comparator;

/**
 * At present, all output keys emitted by {@code AppEngineMapper}s 
 * and consumed by {@code AppEngineReducer}s must implement this 
 * interface.
 * 
 * In the Hadoop framework, Map/Reducer writers supply the framework with the Comparator
 * that imposes the desired ordering on keys output from Mappers.
 * 
 * In the AppEngine implementation, we rely on the datastore to perform our sorting, which 
 * means that we no longer have control over the way in which keys are sorted.
 * 
 * This means that developer's desired ordering must be expressed not with a 
 * {@link Comparator } but rather with a function that encodes the value to a String in
 * such a way that preserves the developer's desired ordering under a lexical comparator.
 * 
 * In other words, if you want your Reducers to receive long integer value in natural order,
 * you can't just use "1", "2", "9", "20", "2000", because the datastore will sort them as 
 * "1", "2", "20", "2000". Nor can you use, in this example, the 
 * {@link Writables#createStringFromWritable(org.apache.hadoop.io.Writable) function because
 * the sign flag is stored in the high bit: {-1, 3000} will be sorted as {3000, -1}
 * 
 * The second constraint on the key encoding is that it must <strong>either</strong> be length 
 * invariant <strong>or</strong> avoid using the "_" character, as the 
 * actually entity key used is a composite of {@link AppEngineJobContext#getJobID()}, 
 * the encoded output key, and the shard number, separated by underscores.
 *  
 * @author alex@bedatadriven (Alex Bertram)
 *
 */
public abstract class OutputKey {
	
	/**
	 *
	 * @return a String encoding of the key's value
	 */
	public abstract String getKeyString();

	
	public abstract void readFromKeyString(String keyString);
	
}

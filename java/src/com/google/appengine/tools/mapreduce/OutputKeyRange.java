package com.google.appengine.tools.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 *  Thin wrapper around a {@code ShardKeyRange} entity in the datastore.
 *
 * The {@code ShardKeyRange} entities maintain the range of keys written by a
 * {@code AppEngineMapper}, per mapper shard.
 *
 *  These are ranges of the keys
 * encoded as Strings, whose ordering is a lexical function of the key's serialized
 * form interpreted as UTF-16, i.e. potentially wacky.
 *
 * @author alex@bedatadriven.com (Alex Bertram)
 */
class OutputKeyRange implements Writable {

  private String minKey;
  private String maxKey;


  OutputKeyRange() {
  }
  
  OutputKeyRange(String minKey, String maxKey) {
	  this.minKey = minKey;
	  this.maxKey = maxKey;
  }

  public static OutputKeyRange aggregate(List<ShardState> shardStates) {
    OutputKeyRange range = new OutputKeyRange();
    for(ShardState shard : shardStates) {
    	if(!shard.getOutputKeyRange().isEmpty()) {
	      range.addKey(shard.getOutputKeyRange().getMinKey());
	      range.addKey(shard.getOutputKeyRange().getMaxKey());
    	}
    }
    return range;
  }

  /**
   *
   * Returns the (lexically) maximum key written by this shard's {@code AppEngineMapper}.
   *
   * @return the maximum key, encoded as a string.
   */
  public String getMaxKey() {
    return maxKey;
  }

  /**
   * Returns the (lexically) minimum key written by this shard's {@code AppEngineMapper}.
   *
   * @return the minimum key, encoded as a String
   */
  public String getMinKey() {
    return minKey;
  }

  public boolean isEmpty() {
    return minKey == null;
  }

  /**
   * Expands the range to include the given key.
   *
   * @param encodedKey the given key, encoded as a String.
   */
  public boolean addKey(String encodedKey) {
    boolean updated = false;
    if(minKey == null || encodedKey.compareTo(minKey) < 0) {
      minKey = encodedKey;
      updated = true;
    }
    if(maxKey == null || encodedKey.compareTo(maxKey) > 0) {
      maxKey = encodedKey;
      updated = true;
    }
    return updated;
  }

  public boolean addKey(OutputKey key) {
    return addKey(key.getKeyString());
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    writeNullableUTF(dataOutput, minKey);
    writeNullableUTF(dataOutput, maxKey);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    minKey = readNullableUTF(dataInput);
    maxKey = readNullableUTF(dataInput);
  }

  private void writeNullableUTF(DataOutput out, String key) throws IOException {
    if(key == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(key);
    }
  }

  private String readNullableUTF(DataInput in) throws IOException {
    if(in.readBoolean()) {           
      return in.readUTF();
    } else {
      return null;
    }
  }
}

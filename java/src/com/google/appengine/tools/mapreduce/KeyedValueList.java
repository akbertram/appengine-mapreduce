package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Thin wrapper around a {@code KeyValueList} entity in the datastore.
 *
 * {@code KeyValueList}s are the intermediate output of an {@code AppEngineMapper} which look
 * like:
 *
 * <pre>
 * Entity Key Name                      Entity Property "v"
 * ---------------                      --------------------
 * {jobId}_{outputKey1}_{shard0}  =>    [ {outputValue1}, {outputValue2}, ... ]
 * {jobId}_{outputKey1}_{shard1}  =>    [ {outputValue1}, {outputValue2}, ... ]
 * {jobId}_{outputKey1}_{shard2}  =>    [ {outputValue1}, {outputValue2}, ... ]
 * {jobId}_{outputKey1}_{shard3}  =>    [ {outputValue1}, {outputValue2}, ... ]
 *
 * {jobId}_{outputKey2}_{shard0}  =>    [ {outputValue1}, {outputValue2}, ... ]
 * {jobId}_{outputKey2}_{shard3}  =>    [ {outputValue1}, {outputValue2}, ... ]
 *
 * {jobId}_{outputKey3}_{shard0}  =>    [ {outputValue1}, {outputValue2}, ... ]
 * </pre>
 *
 * Each time that {@link org.apache.hadoop.mapreduce.Mapper.Context#write(Object, Object)} is called,
 * we get the corresponding {@code KeyValueList} with the composite key of jobId, the written key,
 * and the shard index, and append the written value, serialized using the {@code Writable} interface,
 * to the value blob property, and save it back to the datastore.
 *
 * We take advantage of the fact that the AppEngine datastore sorts entities by
 * their key name to execute the sort/shuffle phase.
 *
 * @author alex@bedatadriven.com (Alex Bertram)
 */
class KeyedValueList {
  public static final String KIND = "KeyedValueList";
  public static final String LIST_PROPERTY = "V";

  private static final int MIN_SHARD = 0;
  private final Entity entity;


  public KeyedValueList(Entity entity) {
    this.entity = entity;
  }

  public Key getEntityKey() {
    return entity.getKey();
  }

  public String getRawKey() {
    return parseRawKey(entity.getKey());
  }

  public static String parseRawKey(Key key) {
    return parseRawKey(key.getName());
  }

  public static String parseRawKey(String name) {
    int start = name.indexOf('_');
    int end = name.lastIndexOf('_');
    if(start == -1 || end == -1) {
      throw new IllegalArgumentException("Expected a key name in the form {jobId}_{serializedKey}_{shardId}");
    }
    return name.substring(start+1, end);
  }

  public Blob getRawValues() {
    return (Blob) entity.getProperty(LIST_PROPERTY);
  }


  private static String keyName(JobID jobID, OutputKey key, int shard) {
    return keyName(jobID, key.getKeyString(), shard);
  }

  @VisibleForTesting
  static String keyName(JobID jobID, String encodedKey, int shard) {
    StringBuilder sb = new StringBuilder()
        .append(jobID.getJtIdentifier())
        .append("_")
        .append(encodedKey)
        .append("_")
        .append(shard);
    return sb.toString();
  }

  public static Key key(TaskAttemptID attemptID, OutputKey key) {
    return KeyFactory.createKey(KIND,
        keyName(attemptID.getJobID(), key, attemptID.getTaskID().getId()));
  }

  public static Key key(JobID jobID, OutputKey key, int shard) {
    return key(jobID, key.getKeyString(), shard);
  }

  private static Key key(JobID jobID, String encodedKey, int shard) {
    return KeyFactory.createKey(KIND, keyName(jobID, encodedKey, shard));
  }

  static InputSplit createSplit(JobID jobId, String encodedMinKey, String encodedMaxKey) {
    return new DatastoreInputSplit(
        key(jobId, encodedMinKey, MIN_SHARD),
        key(jobId, encodedMaxKey, MIN_SHARD));
  }
  
  static InputSplit createFinalSplit(JobID jobId, String encodedMinKey ) {
    return new DatastoreInputSplit(
        key(jobId, encodedMinKey, MIN_SHARD),
        KeyFactory.createKey(KIND, jobId.getJtIdentifier() + "z"));
  }

}

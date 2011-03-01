package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Builder class for {@link com.google.appengine.tools.mapreduce.KeyedValueList} entities that
 * can be persisted across multiple task queue invocations.
 *
 * @author alex@bedatadriven.com (Alex Bertram)
 */
class KeyedValueListBuilder {

  private Key entityKey;
  private ByteArrayOutputStream serializedValueStream;
  private DataOutputStream valueDataStream;
  private OutputKey outputKey;

  KeyedValueListBuilder(OutputKey key, TaskAttemptID attemptId) {
    this.outputKey = key;
    this.entityKey = KeyedValueList.key(attemptId, outputKey);

    serializedValueStream = new ByteArrayOutputStream();
    valueDataStream = new DataOutputStream(serializedValueStream);
  }

  public void init(Blob blob) throws IOException {
    serializedValueStream.write(blob.getBytes());
  }

  /**
   * Restores the builder for the given shard and key from the datastore; or, if the
   * {@code KeyedValueList} entity does not yet exists, creates a new builder.
   *
   * @param datastore
   * @param taskAttemptID the attempt id that uniquely identifies the map/reduce job and shard id
   * @param outputKey the output key (emitted by the {@code AppEngineMapper})
   */
  public static KeyedValueListBuilder restoreOrCreate(DatastoreService datastore,
      TaskAttemptID taskAttemptID, OutputKey outputKey) throws IOException {

    KeyedValueListBuilder list = new KeyedValueListBuilder(outputKey, taskAttemptID);
    list.outputKey = outputKey;

    try {
      Entity entity = datastore.get(list.getEntityKey());
      list.init((Blob) entity.getProperty(KeyedValueList.LIST_PROPERTY));

    } catch (EntityNotFoundException e) {
      // no values yet for this key
      // nothing to initialize
    }

    return list;
  }

  /**
   * Appends a value to the list shard for this outputKey.
   *
   * @param value the value (emitted by {@code AppEngineMapper})
   */
  public int appendValue(Writable value) throws IOException {	
    int oldSize = serializedValueStream.size();
    value.write(valueDataStream);
    return serializedValueStream.size() - oldSize;
  }

  
  public Key getEntityKey() {
    return entityKey;
  }

  public void setEntityKey(Key entityKey) {
    this.entityKey = entityKey;
  }

  public OutputKey getOutputKey() {
    return outputKey;
  }

  public void setOutputKey(OutputKey outputKey) {
    this.outputKey = outputKey;
  }
  
  public Blob getBlob() {
    return new Blob(serializedValueStream.toByteArray());
  }

  /**
   * Persists the builder's state to the datastore
   */
  public Entity toEntity() {
    Entity entity = new Entity(entityKey);
    entity.setProperty(KeyedValueList.LIST_PROPERTY, new Blob(serializedValueStream.toByteArray()));

    return entity;
  }

  /**
   *
   * @return the size of the value list currently in memory, in bytes
   */
  public int size() {
    return serializedValueStream.size();
  }

}

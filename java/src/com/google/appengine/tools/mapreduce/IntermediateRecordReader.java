package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withChunkSize;

/**
 * A {@code RecordReader} which reads the intermediate results of a mapper.
 *
 * @author alex@bedatadriven.com (Alex Bertram)
 */
class IntermediateRecordReader extends RecordReader<String, RawValueList> implements Writable {

  /** The split that this reader iterates over.  */
  private DatastoreInputSplit split;

  /** The lazily generated datastore iterator for this reader. */
  private PeekingIterator<Entity> iterator;

  private KeyedValueList currentShard;
  private Key currentShardKey;
  private String currentKey;
  private RawValueList currentValue;


  public IntermediateRecordReader() {
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Preconditions.checkNotNull(inputSplit);
    if (!(inputSplit instanceof DatastoreInputSplit)) {
      throw new IOException(
          getClass().getName() + " initialized with non-DatastoreInputSplit");
    }


    this.split = (DatastoreInputSplit) inputSplit;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if(iterator == null) {
      createIterator();
    }

    if(!iterator.hasNext()) {
      return false;
    }

    currentShard = new KeyedValueList(iterator.next());
    currentKey = currentShard.getRawKey();
    currentValue = new RawValueList(readListBlobsFromAllShards());

    currentShardKey = currentShard.getEntityKey();

    return true;
  }

  private List<Blob> readListBlobsFromAllShards() {
    ImmutableList.Builder<Blob> list = ImmutableList.builder();

    do {
      list.add(currentShard.getRawValues());

      if(!iterator.hasNext()) {
        return list.build();
      }

      if(!KeyedValueList.parseRawKey(iterator.peek().getKey()).equals(currentKey)) {
        return list.build();
      }

      currentShard = new KeyedValueList(iterator.next());

    } while(true);
  }

  @Override
  public String getCurrentKey() throws IOException, InterruptedException {
    return currentKey;
  }

  @Override
  public RawValueList getCurrentValue() throws IOException, InterruptedException {
    return currentValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    // NO OP
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    currentShardKey = DatastoreSerializationUtil.readKeyOrNull(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    DatastoreSerializationUtil.writeKeyOrNull(out, currentShardKey);
  }

  private void createIterator() {
    Preconditions.checkState(iterator == null);

    Query q = new Query(split.getEntityKind());
    if (currentShardKey == null) {
      q.addFilter(Entity.KEY_RESERVED_PROPERTY, Query.FilterOperator.GREATER_THAN_OR_EQUAL,
          split.getStartKey());
    } else {
      q.addFilter(Entity.KEY_RESERVED_PROPERTY, Query.FilterOperator.GREATER_THAN, currentShardKey);
    }

    if (split.getEndKey() != null) {
      q.addFilter(Entity.KEY_RESERVED_PROPERTY, Query.FilterOperator.LESS_THAN, split.getEndKey());
    }

    q.addSort(Entity.KEY_RESERVED_PROPERTY);

    DatastoreService dsService = DatastoreServiceFactory.getDatastoreService();
    iterator = Iterators.peekingIterator(
        dsService.prepare(q)
            .asQueryResultIterator(withChunkSize(split.getBatchSize())));
  }
}

package com.google.appengine.tools.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.io.CountingInputStream;

/**
 * 
 * Provides an {@code Iterable} wrapper over a set of Blobs containing 
 * serialized {@code Writable} instances.
 * 
 * Note that this implementation follows Hadoop convention in recycling 
 * {@code Writable} instances. 
 * 
 * @author alex@bedatadriven.com
 *
 * @param <T> Writable class
 */
class ShardedRawValueIterable<T extends Writable> implements Iterable<T> {
  private final List<Blob> blobs;
  private final T instance;

  public ShardedRawValueIterable(RawValueList rawValueList, T instance) {
    Preconditions.checkNotNull(instance, "value instance must be provided");
    this.blobs = rawValueList.getBlobs();
    this.instance = instance;
  }

  @Override
  public Iterator<T> iterator() {
    return new ShardedRawValueIterable.ValueIterator<T>(blobs, instance);
  }
  
  private static class ValueIterator<T extends Writable> extends UnmodifiableIterator<T> {
    private DataInputStream dataInputStream;
    private CountingInputStream countingInputStream;
    private final int totalLength;
    private final List<Blob> blobs;
    private T instance;
  
    private ValueIterator(List<Blob> blobs, T instance) {
      this.blobs = blobs;
      this.instance = instance;
      totalLength = sumTotalLength();
      countingInputStream =
          new CountingInputStream(
              new SequenceInputStream(
                  Collections.enumeration(
                      Collections2.transform(blobs, new ShardedRawValueIterable.OpenStream()))));
      dataInputStream =
          new DataInputStream(countingInputStream);
    }
  
    private int sumTotalLength() {
      int length = 0;
      for(Blob blob : blobs) {
        length += blob.getBytes().length;
      }
      return length;
    }
  
    @Override
    public boolean hasNext() {
      return countingInputStream.getCount() < totalLength;
    }
  
    @Override
    public T next() {
      try {
        instance.readFields(dataInputStream);
        return (T) instance;
      } catch (Exception e) {
        throw new RuntimeException("Exception thrown while deserializing value", e);
      }
    }
  }

  private static class OpenStream implements Function<Blob, InputStream> {
    @Override
    public InputStream apply(Blob blob) {
      return new ByteArrayInputStream(blob.getBytes());
    }
  }
}
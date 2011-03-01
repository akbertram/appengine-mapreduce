package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Blob;

import java.util.List;

/**
 * Raw (serialized) list of values from a key's combined list shards.
 *
 * See {@link com.google.appengine.tools.mapreduce.KeyedValueList}
 *
 * @author alex@bedatadriven.com
 */
class RawValueList {
  private final List<Blob> blobs;

  public RawValueList(List<Blob> blobs) {
    this.blobs = blobs;
  }

  public List<Blob> getBlobs() {
    return blobs;
  }
}

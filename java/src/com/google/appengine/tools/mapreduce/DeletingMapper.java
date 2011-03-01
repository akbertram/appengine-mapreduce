package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * An {@code AppEngineMapper} that deletes all input entities.
 *
 * (Used in the cleanup of KeyedValueList entities)
 *
 */
public class DeletingMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {

  @Override
  public void map(Key key, Entity value, Context context) throws IOException, InterruptedException {
    ((AppEngineContext)context).getMutationPool().delete(key);
  }
}

package com.google.appengine.tools.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * An {@code AppEngineMapper} that iterates over the merged
 * intermediary results.
 *
 * The {@code ReducingMapper} drives the reducing phase and is responsible
 * for creating and calling the {@code AppEngineReducer}
 */
public final class ReducingMapper extends AppEngineMapper<String, RawValueList, NullWritable,NullWritable> {
  private AppEngineReducer reducer;
  private AppEngineReducer.AppEngineReducerContext reducerContext;
  private OutputKey currentKey;
  private Writable currentValue;

  @Override
  public void taskSetup(Context context) throws IOException, InterruptedException {
    super.taskSetup(context);

    Class<? extends OutputFormat<?,?>> outputFormatClass = 
        (Class<? extends OutputFormat<?, ?>>) context.getConfiguration().getClass("mapreduce.outputformat.class", NullOutputFormat.class);
    
    OutputFormat<?, ?> outputFormat = ReflectionUtils.newInstance(outputFormatClass, 
        context.getConfiguration());
    
    outputFormat.checkOutputSpecs(context);
       
    
    reducer = createReducer(context);
    reducerContext = AppEngineReducer.getReducerContext(reducer, context, outputFormat.getRecordWriter(context), outputFormat.getOutputCommitter(context), context.getConfiguration(), context.getTaskAttemptID());

    reducer.taskSetup(reducerContext);

    // we can recycle these instances to avoid repeated reflection calls

    currentKey = (OutputKey)ReflectionUtils.newInstance(context.getMapOutputKeyClass(),
        context.getConfiguration());

    currentValue = (Writable)ReflectionUtils.newInstance(context.getMapOutputValueClass(),
        context.getConfiguration());
   
  }

  private AppEngineReducer createReducer(Context context) {
    try {
      return (AppEngineReducer) ReflectionUtils.newInstance(context.getReducerClass(),
          context.getConfiguration());


    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Couldn't find reducer class", e);
    }
  }

  @Override
  public void map(String encodedKey, RawValueList values, Context context) throws IOException, InterruptedException {
    currentKey.readFromKeyString(encodedKey);
    ShardedRawValueIterable valuesIterable = createValueIterable(values);

    reducer.reduce(currentKey, valuesIterable, reducerContext);
  }

  @VisibleForTesting
  ShardedRawValueIterable createValueIterable(RawValueList values) {
    return new ShardedRawValueIterable(values, currentValue);
  }

  @Override
  public void taskCleanup(Context context) throws IOException, InterruptedException {
    reducer.taskCleanup(reducerContext);
    super.taskCleanup(context);
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    reducer.cleanup(reducerContext);
    super.cleanup(context);
  }
}

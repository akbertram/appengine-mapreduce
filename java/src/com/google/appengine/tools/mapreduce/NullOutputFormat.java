package com.google.appengine.tools.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class NullOutputFormat<K, V> extends OutputFormat<K, V> {

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
        return new NullOutputCommitter();
    
  }
  
  private static class NullRecordWriter<K,V> extends RecordWriter<K,V> {

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      // NOOP
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      
      // NOOP
      
    }
  }
 }

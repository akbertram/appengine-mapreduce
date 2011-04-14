package com.google.appengine.tools.mapreduce.fs;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;

public class AppEngineFileSystemTest extends TestCase {
  private LocalServiceTestHelper helper;
  
  @Override
  public void setUp() {
    
    LocalFileServiceTestConfig fileService = new LocalFileServiceTestConfig();
    LocalBlobstoreServiceTestConfig blobstore = new LocalBlobstoreServiceTestConfig();
    blobstore.setBackingStoreLocation("/home/alexander/blobs");
    
    helper = 
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig(), new LocalMemcacheServiceTestConfig(),
          fileService,
          blobstore);
        
    helper.setUp();
  }
  
  public void testMkdirs() throws IOException {

    Configuration conf = new Configuration();
    conf.set("fs.default.name", "appengine://blobstore");
    conf.setClass("fs.appengine.impl", AppEngineFileSystem.class, AppEngineFileSystem.class);
    
    FileSystem fs = FileSystem.get(conf);
    
    fs.mkdirs(new Path("parent/child/folder"));

    FileStatus filesInRoot[] = fs.listStatus(new Path("."));
    assertEquals("files in root count", 1, filesInRoot.length);
    
    FileStatus filesInParent[] = fs.listStatus(new Path("parent"));
    assertEquals("files in parent count", 1, filesInParent.length);

  }
  
  public void testSequenceFile() throws IOException {
    
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "appengine://blobstore");
    conf.setClass("fs.appengine.impl", AppEngineFileSystem.class, AppEngineFileSystem.class);
    
    FileSystem fs = FileSystem.get(conf);
   
    // create individual files to merge
    Path shards[] = new Path[4];
    for(int shard=0;shard!=4;shard++) {
      shards[shard] = new Path("shard" + shard);
      Writer writer = SequenceFile.createWriter(fs, conf, shards[shard], LongWritable.class, IntWritable.class);
      for(int i=0;i!=1000;++i) {
        writer.append(new LongWritable(1000-i), new IntWritable(i));
      }
      writer.close();
    }

    SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, LongWritable.class, IntWritable.class, conf);
    sorter.sort(shards, new Path("sorted"), true);
    
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("sorted"), conf);
    LongWritable key = new LongWritable();
    IntWritable value = new IntWritable();
    long lastKey = Long.MIN_VALUE;
    long count=0;
    while(reader.next(key, value)) {
      System.out.println(key.get() + " => " + value.get());
      if(key.get() < lastKey) {
        throw new AssertionError("not sorted");
      }
      lastKey = key.get();
      count++;
    }
    
    assertEquals(4000, count);
  }
  
}

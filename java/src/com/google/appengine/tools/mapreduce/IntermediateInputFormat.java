package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * A {@code InputFormat} that supplies the result of the Mapping stage as
 * input to the {@link com.google.appengine.tools.mapreduce.ReducingMapper}.
 *
 * @author alex@bedatadriven.com
 */

public class IntermediateInputFormat extends InputFormat<String, RawValueList> {

  private static final Logger logger = Logger.getLogger(IntermediateInputFormat.class.getName());
  
  public static final String MAPPER_JOBID_KEY = "mapreduce.mapper.inputformat.reducerinputformat.mapperjobid";
  public static final String MIN_OUTPUT_KEY = "mapreduce.mapper.inputformat.reducerinputformat.minkey";
  public static final String MAX_OUTPUT_KEY = "mapreduce.mapper.inputformat.reducerinputformat.maxkey";

  private final DatastoreService datastore;

  public IntermediateInputFormat() {
    datastore = DatastoreServiceFactory.getDatastoreService();
  }

  public IntermediateInputFormat(DatastoreService datastore) {
    this.datastore = datastore;
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    AppEngineJobContext context = (AppEngineJobContext) jobContext;
    String mapperJobIdName = context.getConfiguration().get(MAPPER_JOBID_KEY);
    if (mapperJobIdName == null) {
      throw new IOException("Missing the jobID of the mapper");
    }

    JobID mapperJobID = JobID.forName(mapperJobIdName);
    String minKey = context.getConfiguration().get(MIN_OUTPUT_KEY);
    String maxKey = context.getConfiguration().get(MAX_OUTPUT_KEY);


    return getSplits(mapperJobID, minKey, maxKey, context.getMapperShardCount());
  }

  @VisibleForTesting
  List<InputSplit> getSplits(JobID mapperJobID, String minKey, String maxKey, int shardCount) {

    if(minKey.equals(maxKey)) {
      return Collections.singletonList(KeyedValueList.createSplit(mapperJobID, minKey, null));
    }

    logger.severe("minKey = " + minKey +  ", maxKey = " + maxKey);
    
    List<String> splits = StringSplitUtil.splitStrings(minKey, maxKey, shardCount - 1);
    ImmutableList.Builder<InputSplit> listBuilder = new ImmutableList.Builder<InputSplit>();

    String lastKey = minKey;

    for (String currentKey : splits) {
      listBuilder.add(KeyedValueList.createSplit(mapperJobID, lastKey, currentKey));
      lastKey = currentKey;
    }

    // Add in the final split.
    listBuilder.add(KeyedValueList.createFinalSplit(mapperJobID, lastKey));
 
    return listBuilder.build();
  }

  @Override
  public RecordReader<String, RawValueList> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    return new IntermediateRecordReader();
  }
  
  
}

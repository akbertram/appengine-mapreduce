package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.tools.mapreduce.workers.ControllerWorker;
import com.google.appengine.tools.mapreduce.workers.MapperWorker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;

public class MapReduceScheduler {

  private DatastoreService ds = DatastoreServiceFactory.getDatastoreService();


  /**
   * Handle the initial request to start the MapReduce.
   *
   * @return the JobID of the newly created MapReduce or {@code null} if the
   * MapReduce couldn't be created.
   */
  public String handleStart(Configuration conf, String name, HttpServletRequest request) {
    AppEngineJobContext context = new AppEngineJobContext(conf, request, true);

    // Initialize InputSplits
    Class<? extends InputFormat<?, ?>> inputFormatClass;
    try {
      inputFormatClass = context.getInputFormatClass();
    } catch (ClassNotFoundException e) {
      throw new InvalidConfigurationException("Invalid input format class specified.", e);
    }
    InputFormat<?, ?> inputFormat;
    try {
      inputFormat = inputFormatClass.newInstance();
    } catch (InstantiationException e) {
      throw new InvalidConfigurationException(
          "Input format class must have a default constructor.", e);
    } catch (IllegalAccessException e) {
      throw new InvalidConfigurationException(
          "Input format class must have a visible constructor.", e);
    }

    List<InputSplit> splits;
    try {
      splits = inputFormat.getSplits(context);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Thread got interrupted in a single-threaded environment. This shouldn't happen.", e);
    } catch (IOException e) {
      throw new RuntimeException(
          "Got an IOException while trying to make splits", e);
    }

    MapReduceState mrState = MapReduceState.generateInitializedMapReduceState(
        ds, name, context.getJobID(), System.currentTimeMillis());

    mrState.setConfigurationXML(
        ConfigurationXmlUtil.convertConfigurationToXml(
            context.getConfiguration()));

    // Abort if we don't have any splits
    if (splits == null || splits.size() == 0) {
      mrState.setDone();
      mrState.persist();
      return null;
    }

    mrState.persist();
    ControllerWorker.scheduleController(request, context, 0);

    MapperWorker.scheduleShards(request, context, inputFormat, splits);

    return mrState.getJobID();
  }


}

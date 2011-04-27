package com.google.appengine.tools.mapreduce.workers;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.mapreduce.*;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MapperWorker extends Worker {

  private static final Logger log = Logger.getLogger(MapReduceServlet.class.getName());

  private DatastoreService ds = DatastoreServiceFactory.getDatastoreService();


  public static final String MAPPER_WORKER_PATH = "mapperCallback";
  private Clock clock;

  public MapperWorker(Clock clock) {
    this.clock = clock;
  }


  /**
   * Schedules the initial worker callback execution for all mapper shards.
   *
   * @param req the current request
   * @param context this MR's context
   * @param format the input format to use for generating {@code RecordReader}s
   * from the {@code InputSplit}s
   * @param splits all input splits for this MR
   */
  // VisibleForTesting
  public static void scheduleShards(HttpServletRequest req, AppEngineJobContext context,
      InputFormat<?,?> format, List<InputSplit> splits) {
    // TODO(frew): To make life easy for people using InputFormats
    // from general Hadoop, we should add support for grouping
    // InputFormats that generate many splits into a reasonable
    // number of shards.

    // TODO(frew): We will pass along the configuration so that worker tasks
    // don't have to read the MapReduceState whenever task queue supports
    // reasonable size payloads.

    int i = 0;
    for (InputSplit split : splits) {
      Configuration conf = context.getConfiguration();
      TaskAttemptID taskAttemptId = new TaskAttemptID(
          new TaskID(context.getJobID(), true, i), 1);
      ShardState shardState = ShardState.generateInitializedShardState(
          DatastoreServiceFactory.getDatastoreService(), taskAttemptId);
      shardState.setInputSplit(conf, split);
      AppEngineTaskAttemptContext attemptContext = new AppEngineTaskAttemptContext(
          context, shardState, taskAttemptId);
      try {
        RecordReader<?,?> reader = format.createRecordReader(split, attemptContext);
        shardState.setRecordReader(conf, reader);
      } catch (IOException e) {
        throw new RuntimeException(
            "Got an IOException creating a record reader.", e);
      } catch (InterruptedException e) {
        throw new RuntimeException(
            "Got an interrupted exception in a single threaded environment.", e);
      }
      shardState.persist();
      scheduleWorker(req, context, taskAttemptId, 0);
      i++;
    }
  }


  /**
   * Schedules a mapping worker task on the appropriate queue.
   *
   * @param req the current servlet request
   * @param context the context for this MR job
   * @param taskAttemptId the task attempt ID for this worker
   * @param sliceNumber a counter that increments for each sequential, successful
   * task queue invocation
   */
  // VisibleForTesting
  public static void scheduleWorker(HttpServletRequest req, AppEngineJobContext context,
      TaskAttemptID taskAttemptId, int sliceNumber) {
    Preconditions.checkArgument(
        context.getJobID().equals(taskAttemptId.getJobID()),
        "Worker task must be for this MR job");

    String taskName = ("worker_" + taskAttemptId + "__" + sliceNumber).replace('_', '-');
    try {
      context.getWorkerQueue().add(
          TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
          .url(UrlUtil.getBase(req) + MAPPER_WORKER_PATH)
          .param(AppEngineTaskAttemptContext.TASK_ATTEMPT_ID_PARAMETER_NAME,
              "" + taskAttemptId)
              .param(AppEngineJobContext.JOB_ID_PARAMETER_NAME, "" + taskAttemptId.getJobID())
              .param(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME, "" + sliceNumber)
              .taskName(taskName));
    } catch (TaskAlreadyExistsException e) {
      log.warning("Worker task " + taskName + " already exists.");
    }
  }


  /**
   * Does a single task queue invocation's worth of worker work. Also handles
   * calling
   * {@link com.google.appengine.tools.mapreduce.AppEngineMapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * and
   * {@link com.google.appengine.tools.mapreduce.AppEngineMapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * as appropriate.
   */
  @SuppressWarnings("unchecked")
  public <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void handleMapperWorker(HttpServletRequest request, HttpServletResponse response) {

    AppEngineJobContext jobContext;
    AppEngineTaskAttemptContext taskAttemptContext;
    try {
      jobContext = new AppEngineJobContext(request, false);
      taskAttemptContext = new AppEngineTaskAttemptContext(
          request, jobContext, ds);
    } catch (Exception e) {
      log.log(Level.INFO, "Exception retrieving shards from datastore (this job was probably cancelled), aborting shard.", e);
      return;
    }
    DatastorePersistingStatusReporter reporter =
      new DatastorePersistingStatusReporter(taskAttemptContext.getShardState());

    long startTime = clock.currentTimeMillis();
    log.fine("Running worker: " + taskAttemptContext.getTaskAttemptID() + " "
        + jobContext.getSliceNumber());
    try {
      AppEngineMapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper =
        (AppEngineMapper<INKEY,INVALUE,OUTKEY,OUTVALUE>) taskAttemptContext.getMapper();
      InputSplit split = taskAttemptContext.getInputSplit();
      RecordReader<INKEY, INVALUE> reader =
        (RecordReader<INKEY,INVALUE>) taskAttemptContext.getRecordReader(split);
      OutputFormat<OUTKEY, OUTVALUE> outputFormat =
        (OutputFormat<OUTKEY, OUTVALUE>) taskAttemptContext.getOutputFormat();


      RecordWriter<OUTKEY, OUTVALUE> writer = outputFormat.getRecordWriter(taskAttemptContext);
      OutputCommitter outputCommitter = outputFormat.getOutputCommitter(taskAttemptContext);

      AppEngineMapper.AppEngineContext context = getMapperContext(
          taskAttemptContext, mapper, split, reader, writer, reporter);

      if (jobContext.getSliceNumber() == 0) {
        // This is the first invocation for this mapper.
        outputCommitter.setupTask(taskAttemptContext);
        mapper.setup((Mapper.Context) context);
      }

      QuotaConsumer consumer = getQuotaConsumer(taskAttemptContext);

      boolean shouldContinue = processMapper(mapper, (Mapper.Context) context, consumer, startTime);

      if (shouldContinue) {
        taskAttemptContext.getShardState().setRecordReader(jobContext.getConfiguration(), reader);
      } else {
        taskAttemptContext.getShardState().setDone();
      }

      // This persists the shard state including the new record reader.
      reporter.persist();

      writer.close(taskAttemptContext);

      consumer.dispose();

      if (shouldContinue) {
        scheduleWorker(
            request, jobContext, context.getTaskAttemptID(), jobContext.getSliceNumber() + 1);
      } else {
        // This is the last invocation for this mapper.
        mapper.cleanup((Mapper.Context) context);

        // commit the task output
        outputCommitter.commitTask(taskAttemptContext);
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "Map/Reduce shard failed: "+ e.getMessage(), e);
      rethrowIfTransient(e);
      reporter.setStatus(e.getClass().getSimpleName() + ": " + e.getMessage());
      reporter.setError();
      reporter.persist();
    }
  }


  /**
   * Get the mapper context for the current shard. Since there is currently
   * no reducer support, the output values are currently set to {@code null}.
   *
   * @return the newly initialized context
   * @throws java.lang.reflect.InvocationTargetException if the constructor throws an exception
   */
  @SuppressWarnings("unchecked")
  private <INKEY, INVALUE, OUTKEY, OUTVALUE>
  AppEngineMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.AppEngineContext getMapperContext(
      AppEngineTaskAttemptContext taskAttemptContext,
      AppEngineMapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper,
      InputSplit split,
      RecordReader<INKEY, INVALUE> reader,
      RecordWriter<OUTKEY, OUTVALUE> writer,
      StatusReporter reporter) throws InvocationTargetException {
    Constructor<AppEngineMapper.AppEngineContext> contextConstructor;
    try {
      contextConstructor = AppEngineMapper.AppEngineContext.class.getConstructor(
          new Class[]{
              AppEngineMapper.class,
              Configuration.class,
              TaskAttemptID.class,
              RecordReader.class,
              RecordWriter.class,
              OutputCommitter.class,
              StatusReporter.class,
              InputSplit.class
          }
      );
      AppEngineMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.AppEngineContext context =
        contextConstructor.newInstance(
            mapper,
            taskAttemptContext.getConfiguration(),
            taskAttemptContext.getTaskAttemptID(),
            reader,
            writer,
            null, /* not yet implemented */
            reporter,
            split
        );
      return context;
    } catch (SecurityException e) {
      // Since we know the class we're calling, this is strictly a programming error.
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    } catch (NoSuchMethodException e) {
      // Same
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    } catch (IllegalArgumentException e) {
      // There's a small chance this could be a bad supplied argument,
      // but we should validate that earlier.
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    } catch (InstantiationException e) {
      // Programming error
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    } catch (IllegalAccessException e) {
      // Programming error
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    }

  }


  /**
   * Process one task invocation worth of
   * {@link AppEngineMapper#map(Object, Object, org.apache.hadoop.mapreduce.Mapper.Context)}
   * calls. Also handles calling
   * {@link AppEngineMapper#taskSetup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * and
   * {@link AppEngineMapper#taskCleanup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * before and after any map calls made.
   *
   * @return
   * @throws java.io.IOException if the provided {@code mapper} throws such an exception
   * during execution
   *
   * @throws InterruptedException if the provided {@code mapper} throws such an
   * exception during execution
   */
  // VisibleForTesting
  public <INKEY,INVALUE,OUTKEY,OUTVALUE>
  boolean processMapper(
      AppEngineMapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper,
      Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context context,
      QuotaConsumer consumer,
      long startTime)
  throws IOException, InterruptedException {
    boolean shouldShardContinue = true;
    if (consumer.check(1)) {
      mapper.taskSetup(context);
      while (clock.currentTimeMillis() < startTime + PROCESSING_TIME_PER_TASK_MS
          && consumer.consume(1)
          && (shouldShardContinue = context.nextKeyValue())) {
        mapper.map(context.getCurrentKey(), context.getCurrentValue(), context);

        Counter inputRecordsCounter = context.getCounter(
            HadoopCounterNames.MAP_INPUT_RECORDS_GROUP,
            HadoopCounterNames.MAP_INPUT_RECORDS_NAME);
        inputRecordsCounter.increment(1);
      }
      mapper.taskCleanup(context);
    } else {
      log.info("Out of mapper quota. Aborting request until quota is replenished."
          + " Consider increasing " + AppEngineJobContext.MAPPER_INPUT_PROCESSING_RATE_KEY
          + " (default " + AppEngineJobContext.DEFAULT_MAP_INPUT_PROCESSING_RATE
          + ") if you would like your mapper job to complete faster.");
    }

    return shouldShardContinue;
  }



}

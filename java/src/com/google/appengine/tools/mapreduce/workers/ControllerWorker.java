package com.google.appengine.tools.mapreduce.workers;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.mapreduce.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ControllerWorker extends Worker {

  private static final Logger log = Logger.getLogger(ControllerWorker.class.getName());


  // VisibleForTesting
  public static final String CONTROLLER_PATH = "controllerCallback";

  private DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

  private Clock clock;

  public ControllerWorker(Clock clock) {
    this.clock = clock;
  }

  /**
   * Schedules a controller task queue invocation.
   *
   * @param req the current request
   * @param context this MR's job context
   * @param sliceNumber a counter that increments for each sequential, successful
   * task queue invocation
   */
  // VisibleForTesting
  public static void scheduleController(HttpServletRequest req, AppEngineJobContext context, int sliceNumber) {
    String taskName = ("controller_" + context.getJobID() + "__" + sliceNumber).replace('_', '-');
    try {
      context.getControllerQueue().add(
          TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
          .url(UrlUtil.getBase(req) + ControllerWorker.CONTROLLER_PATH)
          .param(AppEngineJobContext.JOB_ID_PARAMETER_NAME, context.getJobID().toString())
          .param(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME, "" + sliceNumber)
          .countdownMillis(2000)
          .taskName(taskName));
    } catch (TaskAlreadyExistsException e) {
      log.warning("Controller task " + taskName + " already exists.");
    }
  }

  /**
   * Handles the logic for a controller task queue invocation.
   *
   * The primary jobs of the controller are to aggregate state from the
   * MR workers (e.g. presenting an overall view of the counters), and to set
   * the quota for MR workers.
   */
  public void handleController(HttpServletRequest request, HttpServletResponse response) {
    AppEngineJobContext context = new AppEngineJobContext(request, false);
    try {
      List<ShardState> shardStates = ShardState.getShardStatesFromJobID(
          ds, context.getJobID());
      MapReduceState mrState = MapReduceState.getMapReduceStateFromJobID(
          ds, context.getJobID());

      if(hasShardsInError(shardStates)) {
        mrState.setActiveShardCount(0);
        mrState.setShardCount(shardStates.size());
        mrState.setError();
        mrState.persist();

        deleteAllShards(shardStates);

      } else {
        List<ShardState> activeShardStates = selectActiveShards(shardStates);

        aggregateState(mrState, shardStates);
        mrState.setActiveShardCount(activeShardStates.size());
        mrState.setShardCount(shardStates.size());
     //   mrState.setOutputKeyRange(OutputKeyRange.aggregate(shardStates));

        if (activeShardStates.size() == 0) {
          mrState.setDone();
        } else {
          refillQuotas(context, mrState, activeShardStates);
        }

        mrState.persist();

        if (MapReduceState.Status.ACTIVE.equals(mrState.getStatus())) {
          scheduleController(request, context, context.getSliceNumber() + 1);
        } else {
          deleteAllShards(shardStates);

       /*   if (!context.isReducer() && context.hasReducer() && !mrState.getOutputKeyRange().isEmpty()) {
            scheduler.handleStart(reducerConfigFromMapperConfig(context.getJobID(), mrState),
                mrState.getName() + " [Reducer]", request);

          } else */
          if (context.hasDoneCallback()) {
            scheduleDoneCallback(
                context.getDoneCallbackQueue(), context.getDoneCallbackUrl(),
                context.getJobID().toString());
          }
        }
      }
    } catch (EntityNotFoundException enfe) {
      log.severe("Couldn't find the state for MapReduce: " + context.getJobID()
          + ". Aborting!");
      return;
    }
  }

  private boolean hasShardsInError(List<ShardState> shardStates) {
    for(ShardState state : shardStates) {
      if(state.getStatus() == ShardState.Status.ERROR) {
        return true;
      }
    }
    return false;
  }


  private void deleteAllShards(List<ShardState> shardStates) {
    List<Key> keys = new ArrayList<Key>();
    for (ShardState shardState : shardStates) {
      keys.add(shardState.getKey());
    }
    ds.delete(keys);
  }

  /**
   * Return all shards with status == ACTIVE.
   */
  private List<ShardState> selectActiveShards(List<ShardState> shardStates) {
    List<ShardState> activeShardStates = new ArrayList<ShardState>();
    for (ShardState shardState : shardStates) {
      if (ShardState.Status.ACTIVE.equals(shardState.getStatus())) {
        activeShardStates.add(shardState);
      }
    }
    return activeShardStates;
  }


  /**
   * Update the current MR state by aggregating information from shard states.
   *
   * @param mrState the current MR state
   * @param shardStates all shard states (active and inactive)
   */
  public void aggregateState(MapReduceState mrState, List<ShardState> shardStates) {
    List<Long> mapperCounts = new ArrayList<Long>();
    Counters counters = new Counters();
    for (ShardState shardState : shardStates) {
      Counters shardCounters = shardState.getCounters();
      // findCounter creates the counter if it doesn't exist.
      mapperCounts.add(shardCounters.findCounter(
          HadoopCounterNames.MAP_INPUT_RECORDS_GROUP,
          HadoopCounterNames.MAP_INPUT_RECORDS_NAME).getValue());

      for (CounterGroup shardCounterGroup : shardCounters) {
        for (Counter shardCounter : shardCounterGroup) {
          counters.findCounter(
              shardCounterGroup.getName(), shardCounter.getName()).increment(
                  shardCounter.getValue());
        }
      }
    }

    log.fine("Aggregated counters: " + counters);
    mrState.setCounters(counters);
    mrState.setProcessedCounts(mapperCounts);
  }


  private void scheduleDoneCallback(Queue queue, String url, String jobId) {
    String taskName = ("done_callback" + jobId).replace('_', '-');
    try {
      queue.add(
          TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
          .url(url)
          .param("job_id", jobId)
          .taskName(taskName));
    } catch (TaskAlreadyExistsException e) {
      log.warning("Done callback task " + taskName + " already exists.");
    }
  }


  /**
   * Refills quotas for all active shards based on the input processing rate.
   *
   * @param context context to get input processing rate from
   * @param mrState the MR state containing the last poll time
   * @param activeShardStates all active shard states
   */
  public void refillQuotas(AppEngineJobContext context, MapReduceState mrState,
      List<ShardState> activeShardStates) {
    if (activeShardStates.size() == 0) {
      return;
    }

    long lastPollTime = mrState.getLastPollTime();
    long currentPollTime = clock.currentTimeMillis();

    int inputProcessingRate = context.getInputProcessingRate();
    long totalQuotaRefill;
    // Initial quota fill
    if (lastPollTime == -1) {
      totalQuotaRefill = inputProcessingRate;
    } else {
      long delta = currentPollTime - lastPollTime;
      totalQuotaRefill = (long) (delta * inputProcessingRate / 1000.0);
    }
    long perShardQuotaRefill = totalQuotaRefill / activeShardStates.size();

    QuotaManager manager = new QuotaManager(MemcacheServiceFactory.getMemcacheService());
    for (ShardState activeShardState : activeShardStates) {
      manager.put(activeShardState.getTaskAttemptID().toString(), perShardQuotaRefill);
    }
    mrState.setLastPollTime(currentPollTime);
  }


}

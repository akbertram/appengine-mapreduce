package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Handles AJAX commands from the user interface
 */
class CommandHandler {

  private static final Logger log = Logger.getLogger(CommandHandler.class.getName());

  private DatastoreService ds = DatastoreServiceFactory.getDatastoreService();


  /**
   *
   */
  private static final int DEFAULT_JOBS_PER_PAGE_COUNT = 50;


  // Command paths
  static final String LIST_JOBS_PATH = "list_jobs";
  static final String LIST_CONFIGS_PATH = "list_configs";
  static final String CLEANUP_JOB_PATH = "cleanup_job";
  static final String ABORT_JOB_PATH = "abort_job";
  static final String GET_JOB_DETAIL_PATH = "get_job_detail";
  static final String START_JOB_PATH = "start_job";

  private final MapReduceScheduler scheduler;


  public CommandHandler(MapReduceScheduler scheduler) {
    this.scheduler = scheduler;
  }

  /**
   * Handles all status page commands.
   */
  public void handleCommand(
      String command, HttpServletRequest request, HttpServletResponse response) {
    JSONObject retValue = null;
    response.setContentType("application/json");
    boolean isPost = "POST".equals(request.getMethod());
    try {
      if (command.equals(LIST_CONFIGS_PATH) && !isPost) {
        MapReduceXml xml;
        try {
          xml = MapReduceXml.getMapReduceXmlFromFile();
          retValue = handleListConfigs(xml);
        } catch (FileNotFoundException e) {
          retValue = new JSONObject();
          retValue.put("status", "Couldn't find mapreduce.xml file");
        }
      } else if (command.equals(LIST_JOBS_PATH) && !isPost) {
        String cursor = request.getParameter("cursor");
        String countString = request.getParameter("count");
        int count = DEFAULT_JOBS_PER_PAGE_COUNT;
        if (countString != null) {
          count = Integer.parseInt(countString);
        }

        retValue = handleListJobs(cursor, count);
      } else if (command.equals(CLEANUP_JOB_PATH) && isPost) {
        retValue = handleCleanupJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(ABORT_JOB_PATH) && isPost) {
        retValue = handleAbortJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(GET_JOB_DETAIL_PATH) && !isPost) {
        retValue = handleGetJobDetail(request.getParameter("mapreduce_id"));
      } else if (command.equals(START_JOB_PATH) && isPost) {
        Map<String, String> templateParams = new TreeMap<String, String>();
        Map httpParams = request.getParameterMap();
        for (Object paramObject : httpParams.keySet()) {
          String param = (String) paramObject;
          if (param.startsWith("mapper_params.")) {
            templateParams.put(param.substring("mapper_params.".length()),
                ((String[]) httpParams.get(param))[0]);
          }
        }
        retValue = handleStartJob(templateParams, ((String []) httpParams.get("name"))[0], request);
      } else {
        response.sendError(404);
        return;
      }
    } catch (Throwable t) {
      log.log(Level.SEVERE, "Got exception while running command", t);
      try {
        retValue = new JSONObject();
        retValue.put("error_class", t.getClass().getName());
        retValue.put("error_message",
            "Full stack trace is available in the server logs. Message: "
            + t.getMessage());
      } catch (JSONException e) {
        throw new RuntimeException("Couldn't create error JSON object", e);
      }
    }
    try {
      retValue.write(response.getWriter());
      response.getWriter().flush();
    } catch (JSONException e) {
      throw new RuntimeException("Couldn't write command response", e);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't write command response", e);
    }
  }

  /**
   * Handle the list_configs AJAX command.
   */
  public JSONObject handleListConfigs(MapReduceXml xml) {
    JSONObject retValue = new JSONObject();
    JSONArray configArray = new JSONArray();
    Set<String> names = xml.getConfigurationNames();
    for (String name : names) {
      String configXml = xml.getTemplateAsXmlString(name);
      ConfigurationTemplatePreprocessor preprocessor =
        new ConfigurationTemplatePreprocessor(configXml);
      configArray.put(preprocessor.toJson(name));
    }
    try {
      retValue.put("configs", configArray);
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null");
    }
    return retValue;
  }

  /**
   * Handle the list_jobs AJAX command.
   */
  public JSONObject handleListJobs(String cursor, int count) {
    List<MapReduceState> states = new ArrayList<MapReduceState>();
    Cursor newCursor = MapReduceState.getMapReduceStates(ds, cursor, count, states);
    JSONArray jobs = new JSONArray();
    for (MapReduceState state : states) {
      jobs.put(state.toJson(false));
    }

    JSONObject retValue = new JSONObject();
    try {
      retValue.put("jobs", jobs);
      if (newCursor != null) {
        retValue.put("cursor", newCursor.toWebSafeString());
      }
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }

    return retValue;
  }

  /**
   * Handle the cleanup_job AJAX command.
   */
  public JSONObject handleCleanupJob(String jobId) {
    JSONObject retValue = new JSONObject();
    try {
      try {
        MapReduceState.getMapReduceStateFromJobID(ds, JobID.forName(jobId)).delete();
        retValue.put("status", "Successfully deleted requested job.");
      } catch (IllegalArgumentException e) {
        retValue.put("status", "Couldn't find requested job.");
      } catch (EntityNotFoundException e) {
        retValue.put("status", "Couldn't find requested job.");
      }
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }
    return retValue;
  }

  /**
   * Handle the abort_job AJAX command.
   */
  public JSONObject handleAbortJob(String jobId) {
    MapReduceState state;
    try {
      state = MapReduceState.getMapReduceStateFromJobID(ds, JobID.forName(jobId));
    } catch (EntityNotFoundException e) {
      throw new IllegalArgumentException("Couldn't find MapReduce for id:" + jobId, e);
    }
    state.abort();
    return new JSONObject();
  }

  /**
   * Handle the get_job_detail AJAX command.
   */
  public JSONObject handleGetJobDetail(String jobId) {
    MapReduceState state;
    try {
      state = MapReduceState.getMapReduceStateFromJobID(ds, JobID.forName(jobId));
    } catch (EntityNotFoundException e) {
      throw new IllegalArgumentException("Couldn't find MapReduce for id:" + jobId, e);
    }
    return state.toJson(true);
  }

  /**
   * Handle the start_job AJAX command.
   */
  public JSONObject handleStartJob(Map<String, String> params, String name,
      HttpServletRequest request) {
    try {
      MapReduceXml mrXml = MapReduceXml.getMapReduceXmlFromFile();
      Configuration configuration = mrXml.instantiateConfiguration(name, params);
      // TODO(frew): What should we be doing here for error handling?
      String jobId = scheduler.handleStart(configuration, name, request);
      JSONObject retValue = new JSONObject();
      try {
        retValue.put("mapreduce_id", jobId);
      } catch (JSONException e) {
        throw new RuntimeException("Hard-coded string is null");
      }
      return retValue;
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Couldn't find mapreduce.xml", e);
    }
  }


}

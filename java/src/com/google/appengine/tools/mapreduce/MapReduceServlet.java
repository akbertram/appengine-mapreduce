/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.tools.mapreduce.workers.ControllerWorker;
import com.google.appengine.tools.mapreduce.workers.MapperWorker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servlet for all MapReduce API related functions.
 *
 * This should be specified as the handler for MapReduce URLs in web.xml.
 * For instance:
 * <pre>
 * {@code
 * <servlet>
 *   <servlet-name>mapreduce</servlet-name>
 *   <servlet-class>com.google.appengine.tools.mapreduce.MapReduceServlet</servlet-class>
 * </servlet>
 * <servlet-mapping>
 *   <servlet-name>mapreduce</servlet-name>
 *   <url-pattern>/mapreduce/*</url-pattern>
 * </servlet-mapping>
 * }
 *
 * Generally you'll want this handler to be protected by an admin security constraint
 * (see <a
 * href="http://code.google.com/appengine/docs/java/config/webxml.html#Security_and_Authentication">
 * Security and Authentication</a>)
 * for more details.
 * </pre>
 *
 * @author frew@google.com (Fred Wulff)
 */
public class MapReduceServlet extends HttpServlet {

  private static final Logger log = Logger.getLogger(MapReduceServlet.class.getName());



  static final String START_PATH = "start";
  static final String COMMAND_PATH = "command";


  private DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
  private Clock clock = new SystemClock();

  MapReduceScheduler scheduler = new MapReduceScheduler();
  CommandHandler commandHandler = new CommandHandler(scheduler);
  ControllerWorker controllerWorker = new ControllerWorker(clock);
  MapperWorker mapperWorker = new MapperWorker(clock);

  /**
   * Returns the handler portion of the URL path.
   * 
   * For example, getHandler(https://www.google.com/foo/bar) -> bar
   * Note that for command handlers,
   * getHandler(https://www.google.com/foo/command/bar) -> command/bar
   */
  static String getHandler(HttpServletRequest request) {
    String requestURI = request.getRequestURI();
    return requestURI.substring(UrlUtil.getDividingIndex(requestURI) + 1);
  }

  /**
   * Checks to ensure that the current request was sent via the task queue.
   *
   * If the request is not in the task queue, returns false, and sets the
   * response status code to 403. This protects against CSRF attacks against
   * task queue-only handlers.
   *
   * @return true if the request is a task queue request
   */
  private boolean checkForTaskQueue(HttpServletRequest request, HttpServletResponse response) {
    if (request.getHeader("X-AppEngine-QueueName") == null) {
      log.log(Level.SEVERE, "Received unexpected non-task queue request. Possible CSRF attack.");
      try {
        response.sendError(
            HttpServletResponse.SC_FORBIDDEN, "Received unexpected non-task queue request.");
      } catch (IOException ioe) {
        throw new RuntimeException("Encountered error writing error", ioe);
      }
      return false;
    }
    return true;
  }

  /**
   * Checks to ensure that the current request was sent via an AJAX request.
   *
   * If the request was not sent by an AJAX request, returns false, and sets
   * the response status code to 403. This protects against CSRF attacks against
   * AJAX only handlers.
   *
   * @return true if the request is a task queue request
   */
  private boolean checkForAjax(HttpServletRequest request, HttpServletResponse response) {
    if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With"))) {
      log.log(
          Level.SEVERE, "Received unexpected non-XMLHttpRequest command. Possible CSRF attack.");
      try {
        response.sendError(HttpServletResponse.SC_FORBIDDEN,
        "Received unexpected non-XMLHttpRequest command.");
      } catch (IOException ioe) {
        throw new RuntimeException("Encountered error writing error", ioe);
      }
      return false;
    }
    return true;
  }

  /**
   * Handles all MapReduce callbacks.
   */
  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response) {
    String handler = MapReduceServlet.getHandler(request);
    if (handler.startsWith(ControllerWorker.CONTROLLER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      controllerWorker.handleController(request, response);
    } else if (handler.startsWith(MapperWorker.MAPPER_WORKER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      mapperWorker.handleMapperWorker(request, response);
    } else if (handler.startsWith(START_PATH)) {
      // We don't add a GET handler for this one, since we're expecting the user 
      // to POST the whole XML specification.
      // TODO(frew): Make name customizable.
      // TODO(frew): Add ability to specify a redirect.
      scheduler.handleStart(
          ConfigurationXmlUtil.getConfigurationFromXml(request.getParameter("configuration")),
          "Automatically run request", request);
    } else if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      commandHandler.handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else {
      throw new RuntimeException(
      "Received an unknown MapReduce request handler. See logs for more detail.");
    }
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    String handler = MapReduceServlet.getHandler(request);
    if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      commandHandler.handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else {
      handleStaticResources(handler, response);
    }
  }



  private Configuration reducerConfigFromMapperConfig(JobID mapperJobID, MapReduceState mapperState) {

    Configuration reducerConfig = ConfigurationXmlUtil
    .getConfigurationFromXml(mapperState.getConfigurationXML());

    // define input for the reducing "mapper" as a function of our just completed mapping job
    reducerConfig.set("mapreduce.inputformat.class", IntermediateInputFormat.class.getName());
    reducerConfig.set(IntermediateInputFormat.MAPPER_JOBID_KEY, mapperJobID.toString());
    reducerConfig.set(IntermediateInputFormat.MIN_OUTPUT_KEY, mapperState.getOutputKeyRange().getMinKey());
    reducerConfig.set(IntermediateInputFormat.MAX_OUTPUT_KEY, mapperState.getOutputKeyRange().getMaxKey());

    // defined the "mapper" that handles the reduction process
    reducerConfig.set("mapreduce.map.class", ReducingMapper.class.getName());

    if(reducerConfig.get(AppEngineJobContext.REDUCER_SHARD_COUNT_KEY) != null) {
      reducerConfig.set(AppEngineJobContext.MAPPER_SHARD_COUNT_KEY,
          reducerConfig.get(AppEngineJobContext.REDUCER_SHARD_COUNT_KEY));
    }
    return reducerConfig;
  }

  // VisibleForTesting
  void setClock(Clock clock) {
    this.clock = clock;
  }



  /**
   * Handle serving of static resources (which we do dynamically so users
   * only have to add one entry to their web.xml).
   */
  public void handleStaticResources(String handler, HttpServletResponse response) {
    String fileName = null;
    if (handler.equals("status")) {
      response.setContentType("text/html");
      fileName = "overview.html";
    } else if (handler.equals("detail")) {
      response.setContentType("text/html");
      fileName = "detail.html";
    } else if (handler.equals("base.css")) {
      response.setContentType("text/css");
      fileName = "base.css";
    } else if (handler.equals("jquery.js")) {
      response.setContentType("text/javascript");
      fileName = "jquery-1.4.2.min.js";
    } else if (handler.equals("status.js")) {
      response.setContentType("text/javascript");
      fileName = "status.js";
    } else {
      try {
        response.sendError(404);
      } catch (IOException e) {
        throw new RuntimeException("Encountered error sending 404", e);
      }
      return;
    }

    response.setHeader("Cache-Control", "public; max-age=300");

    try {
      InputStream resourceStream = MapReduceServlet.class.getResourceAsStream(
          "/com/google/appengine/tools/mapreduce/" + fileName);
      OutputStream responseStream = response.getOutputStream();
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = resourceStream.read(buffer)) != -1) {
        responseStream.write(buffer, 0, bytesRead);
      }
      responseStream.flush();
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Couldn't find static file for MapReduce library", e);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't read static file for MapReduce library", e);
    }
  }
}

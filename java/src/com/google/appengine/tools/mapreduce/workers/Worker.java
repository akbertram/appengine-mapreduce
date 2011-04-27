package com.google.appengine.tools.mapreduce.workers;

import com.google.appengine.api.blobstore.BlobstoreFailureException;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.memcache.MemcacheServiceException;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.tools.mapreduce.AppEngineTaskAttemptContext;
import com.google.appengine.tools.mapreduce.QuotaConsumer;
import com.google.appengine.tools.mapreduce.QuotaManager;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.DeadlineExceededException;

public abstract class Worker {


  /**
   * Default amount of quota to divvy up per controller execution.
   */
  public static final int DEFAULT_QUOTA_BATCH_SIZE = 20;


  // Amount of time to spend on actual map() calls per task execution.
  public static final int PROCESSING_TIME_PER_TASK_MS = 10000;

  /**
   * Get the QuotaConsumer for current shard.
   */
  protected QuotaConsumer getQuotaConsumer(AppEngineTaskAttemptContext taskAttemptContext) {
    QuotaManager manager = new QuotaManager(MemcacheServiceFactory.getMemcacheService());
    QuotaConsumer consumer = new QuotaConsumer(
        manager, taskAttemptContext.getTaskAttemptID().toString(), DEFAULT_QUOTA_BATCH_SIZE);
    return consumer;
  }



  protected void rethrowIfTransient(Exception e) {
    if (e instanceof BlobstoreFailureException) {
      throw (BlobstoreFailureException)e;
    }
    if(e instanceof DatastoreTimeoutException) {
      throw (DatastoreTimeoutException)e;
    }
    if(e instanceof DeadlineExceededException) {
      throw (DeadlineExceededException)e;
    }
    if(e instanceof MemcacheServiceException) {
      throw (MemcacheServiceException)e;
    }
    if(e instanceof ApiProxy.ApiDeadlineExceededException) {
      throw (ApiProxy.ApiDeadlineExceededException)e;
    }
    if(e instanceof ApiProxy.CancelledException) {
      throw (ApiProxy.CancelledException)e;
    }
    if(e.getCause() instanceof Exception) {
      rethrowIfTransient((Exception) e.getCause());
    }
  }
}

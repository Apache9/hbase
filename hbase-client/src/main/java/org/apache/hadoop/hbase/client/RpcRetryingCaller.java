/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DoNotRetryNowIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Dynamic rather than static so can set the generic return type appropriately.
 */
@InterfaceAudience.Private
public class RpcRetryingCaller<T> {

  private static final Log LOG = LogFactory.getLog(RpcRetryingCaller.class);

  /** How many retries are allowed before we start to log */
  private final int startLogErrorsCnt;

  private final long pause;

  private final int retries;

  private final boolean ignoreThrottlingException;

  private final int rpcTimeout;

  public RpcRetryingCaller(long pause, int retries, int startLogErrorsCnt, int rpcTimeout) {
    this(pause, retries, startLogErrorsCnt, false, rpcTimeout);
  }

  public RpcRetryingCaller(long pause, int retries, int startLogErrorsCnt,
      boolean ignoreThrottlingException, int rpcTimeout) {
    this.pause = pause;
    this.retries = retries;
    this.startLogErrorsCnt = startLogErrorsCnt;
    this.ignoreThrottlingException = ignoreThrottlingException;
    this.rpcTimeout = rpcTimeout;
  }

  private int getRemainingTime(long globalStartTime, int callTimeout, int tries)
      throws CallTimeoutException {
    long duration = EnvironmentEdgeManager.currentTimeMillis() - globalStartTime;
    if (callTimeout <= duration) {
      throw new CallTimeoutException(
          "callTimeout=" + callTimeout + ", callDuration=" + duration + ", tries=" + tries);
    }
    return (int) Math.min(rpcTimeout, callTimeout - duration);
  }

  public synchronized T callWithRetries(RetryingCallable<T> callable)
      throws IOException, RuntimeException {
    return callWithRetries(callable, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  }

  /**
   * Retries if invocation fails.
   * @param callTimeout Timeout for this call
   * @param callable The {@link RetryingCallable} to run.
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "SWL_SLEEP_WITH_LOCK_HELD",
      justification = "na")
  public synchronized T callWithRetries(RetryingCallable<T> callable, int callTimeout)
      throws IOException, RuntimeException {
    long globalStartTime = EnvironmentEdgeManager.currentTimeMillis();
    List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions =
        new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>();
    for (int tries = 0;; tries++) {
      long expectedSleep = 0;
      try {
        // bad cache entries are cleared in the call to RetryingCallable#throwable() in catch block
        callable.prepare(getRemainingTime(globalStartTime, callTimeout, tries), tries != 0);
        return callable.call(getRemainingTime(globalStartTime, callTimeout, tries));
      } catch (Throwable t) {
        if (tries > startLogErrorsCnt) {
          LOG.info("Call exception, tries=" + tries + ", retries=" + retries + ", retryTime="
              + (EnvironmentEdgeManager.currentTimeMillis() - globalStartTime) + "ms, msg="
              + callable.getExceptionMessageAdditionalDetail(), t);
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("Call exception, tries=" + tries + ", retries=" + retries + ", retryTime="
              + (EnvironmentEdgeManager.currentTimeMillis() - globalStartTime) + "ms, msg="
              + callable.getExceptionMessageAdditionalDetail(), t);
        }
        // translateException throws exception when should not retry: i.e. when request is bad.
        t = translateException(t);
        callable.throwable(t, retries != 1);
        RetriesExhaustedException.ThrowableWithExtraContext qt =
            new RetriesExhaustedException.ThrowableWithExtraContext(t,
                EnvironmentEdgeManager.currentTimeMillis(), toString());
        exceptions.add(qt);
        ExceptionUtil.rethrowIfInterrupt(t);
        if (tries >= retries - 1 && !handleException(t)) {
          throw new RetriesExhaustedException(tries, exceptions);
        }

        expectedSleep = calculateExpectedSleep(callable, tries, t);

        // If, after the planned sleep, there won't be enough time left, we stop now.
        long duration = singleCallDuration(globalStartTime, expectedSleep);
        if (duration > callTimeout) {
          String msg = "callTimeout=" + callTimeout + ", callDuration="
              + (EnvironmentEdgeManager.currentTimeMillis() - globalStartTime) + ", expectedSleep="
              + expectedSleep + ", tries=" + tries
              + ": " + callable.getExceptionMessageAdditionalDetail();
          throw new CallTimeoutException(msg, new RetriesExhaustedException(tries, exceptions));
        }
      }
      LOG.info("Sleep for next retry, tries=" + tries + ", retries=" + retries + ", sleepTime="
          + expectedSleep);
      try {
        Thread.sleep(expectedSleep);
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Interrupted after " + tries + " tries  on " + retries);
      }
    }
  }

  /**
   * @param callable The {@link RetryingCallable} to run.
   * @param tries Have retried number
   * @param t the throwable to analyze
   * @return expected sleep time
   */
  private long calculateExpectedSleep(RetryingCallable<T> callable, final int tries, Throwable t) {
    if (t instanceof DoNotRetryNowIOException) {
      return ((DoNotRetryNowIOException) t).getWaitInterval()
          + ConnectionUtils.addJitter(pause, 1.0f);
    } else {
      // If the server is dead, we need to wait a little before retrying, to give
      // a chance to the regions to be
      // get right pause time, start by RETRY_BACKOFF[0] * pause
      return callable.sleep(pause, tries);
    }
  }

  /**
   * @param expectedSleep
   * @return Calculate how long a single call took
   */
  private long singleCallDuration(long globalStartTime, long expectedSleep) {
    return (EnvironmentEdgeManager.currentTimeMillis() - globalStartTime) + expectedSleep;
  }

  /**
   * Call the server once only. {@link RetryingCallable} has a strange shape so we can do retrys.
   * Use this invocation if you want to do a single call only (A call to
   * {@link RetryingCallable#call()} will not likely succeed).
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  public T callWithoutRetries(RetryingCallable<T> callable, int callTimeout)
      throws IOException, RuntimeException {
    // The code of this method should be shared with withRetries.
    long globalStartTime = EnvironmentEdgeManager.currentTimeMillis();
    try {
      callable.prepare(getRemainingTime(globalStartTime, callTimeout, 0), false);
      return callable.call(getRemainingTime(globalStartTime, callTimeout, 0));
    } catch (Throwable t) {
      Throwable t2 = translateException(t);
      ExceptionUtil.rethrowIfInterrupt(t2);
      // It would be nice to clear the location cache here.
      if (t2 instanceof IOException) {
        throw (IOException) t2;
      } else {
        throw new RuntimeException(t2);
      }
    }
  }

  /**
   * Get the good or the remote exception if any, throws the DoNotRetryIOException.
   * @param t the throwable to analyze
   * @return the translated exception, if it's not a DoNotRetryIOException
   * @throws DoNotRetryIOException - if we find it, we throw it instead of translating.
   */
  static Throwable translateException(Throwable t) throws DoNotRetryIOException {
    if (t instanceof UndeclaredThrowableException) {
      if (t.getCause() != null) {
        t = t.getCause();
      }
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException) t).unwrapRemoteException();
    }
    if (t instanceof LinkageError) {
      throw new DoNotRetryIOException(t);
    }
    if (t instanceof ServiceException) {
      ServiceException se = (ServiceException) t;
      Throwable cause = se.getCause();
      if (cause != null && cause instanceof DoNotRetryIOException) {
        throw (DoNotRetryIOException) cause;
      }
      // Don't let ServiceException out; its rpc specific.
      // cause could be a RemoteException so go around again.
      t = translateException(cause);
    } else if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException) t;
    } else if (t.getCause() != null && t.getCause() instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException) t.getCause();
    }
    return t;
  }

  private boolean handleException(Throwable t) {
    if (ignoreThrottlingException && (t instanceof ThrottlingException
        || t instanceof RpcThrottlingException)) {
      return true;
    }
    return false;
  }
}

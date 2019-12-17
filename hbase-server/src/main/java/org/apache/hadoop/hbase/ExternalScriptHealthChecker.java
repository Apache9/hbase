/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * A utility for executing an external script that checks the health of
 * the node. An example script can be found at
 * <tt>src/main/sh/healthcheck/healthcheck.sh</tt> in the
 * <tt>hbase-examples</tt> module.
 */
class ExternalScriptHealthChecker implements HealthChecker {

  private static Log LOG = LogFactory.getLog(ExternalScriptHealthChecker.class);
  private ShellCommandExecutor shexec = null;
  private String exceptionStackTrace;

  /** Pattern used for searching in the output of the node health script */
  static private final String ERROR_PATTERN = "ERROR";
  private ArrayList<String> execScript = new ArrayList<String>();
  private final long scriptTimeout;
  private final Configuration config;

  public ExternalScriptHealthChecker(Configuration conf) {
    this.config = conf;
	  String healthCheckScript = config.get(HConstants.HEALTH_SCRIPT_LOC);
    scriptTimeout = config.getLong(HConstants.HEALTH_SCRIPT_TIMEOUT,
        HConstants.DEFAULT_HEALTH_SCRIPT_TIMEOUT);
    long period =
        config.getLong(HConstants.HEALTH_CHECKER_PERIOD, HConstants.DEFAULT_HEALTH_CHECKER_PERIOD);
    String clusterName = config.get(HConstants.CLUSTER_NAME, "");
    init(healthCheckScript, scriptTimeout, period, clusterName);
  }

  /**
   * Initialize.
   *
   * @healthCheckScript: The bash script location for health checker
   */
  public void init(String healthCheckScript, long timeout, long period, String clusterName) {
    if (StringUtils.isNotBlank(healthCheckScript)) {
      execScript.add("/bin/bash");
      execScript.add(healthCheckScript);
      execScript.add(String.valueOf(period));
      execScript.add(clusterName);

      this.shexec =
          new ShellCommandExecutor(execScript.toArray(new String[execScript.size()]), null, null,
              scriptTimeout);
    }
	  LOG.info("ExternalScriptHealthChecker initialized with script at " + healthCheckScript +
      ", timeout=" + timeout);
  }

  @Override
  public HealthReport checkHealth() {
    if (execScript.isEmpty()) {
      return new HealthReport(HealthCheckerExitStatus.SUCCESS, "Exec script is empty");
    }
    HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
    try {
      // Calling this execute leaves around running executor threads.
      shexec.execute();
      LOG.info("Script output: " + shexec.getOutput());
    } catch (ExitCodeException e) {
      // ignore the exit code of the script
      LOG.warn("Caught exception : " + e + ",exit code:" + e.getExitCode());
      status = HealthCheckerExitStatus.FAILED_WITH_EXIT_CODE;
    } catch (IOException e) {
      LOG.warn("Caught exception : " + e);
      status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
      exceptionStackTrace = org.apache.hadoop.util.StringUtils.stringifyException(e);
    } finally {
      if (shexec.isTimedOut()) {
        status = HealthCheckerExitStatus.TIMED_OUT;
      }
      if (status == HealthCheckerExitStatus.SUCCESS) {
        if (hasErrors(shexec.getOutput())) {
          status = HealthCheckerExitStatus.FAILED;
        }
      }
    }
    // Here we ignore the errors, because we just want to post message to falcon.
    // See the script for more details.
    if (StringUtils.isNotBlank(config.get(HConstants.CLUSTER_NAME))) {
      return new HealthReport(HealthCheckerExitStatus.SUCCESS, getHealthReport(status));
    } else {
      return new HealthReport(status, getHealthReport(status));
    }
  }

  private boolean hasErrors(String output) {
    String[] splits = output.split("\n");
    for (String split : splits) {
      if (split.contains(ERROR_PATTERN)) {
        return true;
      }
    }
    return false;
  }

  private String getHealthReport(HealthCheckerExitStatus status){
    String healthReport = null;
    switch (status) {
    case SUCCESS:
      healthReport = "Server is healthy.";
      break;
    case TIMED_OUT:
      healthReport = "Health script timed out";
      break;
    case FAILED_WITH_EXCEPTION:
      healthReport = exceptionStackTrace;
      break;
    case FAILED_WITH_EXIT_CODE:
      healthReport = "Health script failed with exit code.";
      break;
    case FAILED:
      healthReport = shexec.getOutput();
      break;
    }
    return healthReport;
  }
}

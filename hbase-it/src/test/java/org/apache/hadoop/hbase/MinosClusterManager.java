/**
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

import com.google.common.base.Preconditions;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * Cluster manager based on minor command line.
 */
@InterfaceAudience.Private
public class MinosClusterManager extends Configured implements ClusterManager {

  private static final Log LOG = LogFactory.getLog(MinosClusterManager.class);

  private final static String DEPLOY_CMD_FORMAT_FOR_HOST = "deploy %s hbase %s --job=%s --host=%s";

  private String clusterName;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      // Configured gets passed null before real conf. Why? I don't know.
      return;
    }
    clusterName = Preconditions.checkNotNull(conf.get("hbase.cluster.name"),
      "must set 'hbase.cluster.name'");
  }

  @Override
  public void start(ServiceType service, String hostname, int port) throws IOException {
    runDeployCmd("start", service.getName(), hostname, port, true);
  }

  @Override
  public void stop(ServiceType service, String hostname, int port) throws IOException {
    runDeployCmd("stop", service.getName(), hostname, port, true);
  }

  @Override
  public void restart(ServiceType service, String hostname, int port) throws IOException {
    runDeployCmd("restart", service.getName(), hostname, port, true);
  }

  @Override
  public void kill(ServiceType service, String hostname, int port) throws IOException {
    stop(service, hostname, port);
  }

  @Override
  public void suspend(ServiceType service, String hostname, int port) throws IOException {
    stop(service, hostname, port);
  }

  @Override
  public void resume(ServiceType service, String hostname, int port) throws IOException {
    start(service, hostname, port);
  }

  @Override
  public boolean isRunning(ServiceType service, String hostname, int port) throws IOException {
    String output = runDeployCmd("show", service.getName(), hostname, port, false);
    return isRunning(output);
  }

  public String runDeployCmd(String action, String job, String hostname, int port,
      boolean skipConfirm) throws IOException {
    String cmd = String.format(DEPLOY_CMD_FORMAT_FOR_HOST, action, clusterName, job, hostname);
    return runDeployCmdWithSkipConfirm(cmd, skipConfirm);
  }

  public String runDeployCmdWithSkipConfirm(String cmd, boolean skipConfirm) throws IOException {
    if (skipConfirm) {
      cmd += " --skip_confirm";
    }
    LOG.info("Start Executing deploy cmd: " + cmd);
    String output = exec(cmd);
    LOG.info("Executed deploy cmd: " + cmd + ". The output is: \n" + output);
    return output;
  }

  private static boolean isRunning(String output) {
    return !output.contains("is STOPPED") && !output.contains("is EXITED")
        && !output.contains("is BACKOFF") && !output.contains("is FATAL")
        && !output.contains("is STARTING");
  }

  private static String exec(final String cmd) throws IOException {
    String[] cmds = new String[] { "bash", "-c", cmd };
    return ShellCommandExecutor.execCommand(cmds);
  }
}

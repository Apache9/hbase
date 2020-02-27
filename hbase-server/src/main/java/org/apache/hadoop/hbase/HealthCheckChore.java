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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.xiaomi.infra.hbase.regionserver.chore.DirectHealthChecker;
import com.xiaomi.infra.hbase.util.MailUtils;

/**
 * The Class HealthCheckChore for running health checker regularly.
 */
 public class HealthCheckChore extends Chore {
  private static Log LOG = LogFactory.getLog(HealthCheckChore.class);
  private final List<HealthChecker> healthCheckerList = new ArrayList<>();
	private final Stoppable stopper;
	private final int threshold;
  private final long windowWidth;
  private int numTimesUnhealthy = 0;
  private long startWindow;

	public HealthCheckChore(int sleepTime, Stoppable stopper, Configuration conf) {
		super("HealthCheckerChore", sleepTime, stopper);
    LOG.info("Health Check Chore runs every " + StringUtils.formatTime(sleepTime));
		this.stopper = stopper;

		if (stopper instanceof HRegionServer) {
			HRegionServer regionserver = (HRegionServer) stopper;
			healthCheckerList.add(new DirectHealthChecker(regionserver));
		}
		healthCheckerList.add(new ExternalScriptHealthChecker(conf));
		this.threshold = conf.getInt(HConstants.HEALTH_FAILURE_THRESHOLD,
      HConstants.DEFAULT_HEALTH_FAILURE_THRESHOLD);
    this.windowWidth = (long)this.threshold * (long)sleepTime;
  }

  @Override
  protected void chore() {
	  if (healthCheckerList.isEmpty()) {
		  return;
	  }
	  boolean isHealthy = healthCheckerList.stream().allMatch(healthChecker -> {
		  HealthReport healthReport = healthChecker.checkHealth();
		  LOG.info("Health status  " + healthReport.getHealthReport());
		  return HealthCheckerExitStatus.SUCCESS == healthChecker.checkHealth().getStatus();
	  });
    if (!isHealthy) {
      boolean needToStop = decideToStop();
      if (needToStop) {
	      LOG.warn("The numTimesUnhealthy=" + numTimesUnhealthy + ",threshold=" + threshold
			      + ", so STOP!");
	      String errorMessage =
			      "The  node reported unhealthy " + threshold + " number of times consecutively.";
	      if (stopper instanceof HRegionServer) {
		      HRegionServer server = (HRegionServer) stopper;
		      processBeforeStopForRegionServer(server, errorMessage);
		      server.abort(errorMessage);
	      } else {
		      this.stopper.stop(errorMessage);
	      }
      }
    }
  }

  @VisibleForTesting
  boolean decideToStop() {
		++numTimesUnhealthy;
    long now = EnvironmentEdgeManager.currentTimeMillis();
    if (now - startWindow >= windowWidth) {
      setStartWindow(now);
      numTimesUnhealthy = 1;
    }
    return numTimesUnhealthy >= threshold;
  }

  @VisibleForTesting
  void setStartWindow(long startWindow) {
		this.startWindow = startWindow;
  }

  private void processBeforeStopForRegionServer(HRegionServer regionServer, String errorMessage) {
	  regionServer.moveOutRegions("health checker failure");
	  String clusterName = regionServer.getClusterName();
	  String subject =
			  "HealthChecker for cluster " + clusterName + " abort regionserver " + regionServer
					  .getServerName().getHostAndPort();
	  if ((clusterName != null) && !"".equals(clusterName)) { // exclude UT
		  MailUtils.sendMail(regionServer.getConfiguration() ,HConstants.MAIL_TO, subject, errorMessage);
	  }
  }

}

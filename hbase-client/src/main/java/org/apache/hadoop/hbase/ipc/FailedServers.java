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
package org.apache.hadoop.hbase.ipc;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * A class to manage a list of servers that failed recently.
 */
@InterfaceAudience.Private
public class FailedServers {

  public static final String FAILED_SERVER_EXPIRY_KEY = "hbase.ipc.client.failed.servers.expiry";
  public static final int FAILED_SERVER_EXPIRY_DEFAULT = 2000;

  static class FailedServer {
    public long expiry;
    public String address;
    public Throwable cause;
    public long failedTimeStamp;

    public FailedServer(long expiry, String address, Throwable cause, long failedTimeStamp) {
      this.expiry = expiry;
      this.address = address;
      this.cause = cause;
      this.failedTimeStamp = failedTimeStamp;
    }

    public String formatFailedTimeStamp() {
      String failedTimeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          .format(new Date(this.failedTimeStamp));
      return "(put to failed server list at: " + failedTimeStr + ")";
    }
  }

  private final LinkedList<FailedServer> failedServers = new LinkedList<FailedServer>();
  private final int recheckServersTimeout;

  public FailedServers(Configuration conf) {
    this.recheckServersTimeout = conf.getInt(FAILED_SERVER_EXPIRY_KEY,
      FAILED_SERVER_EXPIRY_DEFAULT);
  }

  /**
   * Add an address to the list of the failed servers list.
   */
  public synchronized void addToFailedServers(InetSocketAddress address) {
    addToFailedServers(address, null);
  }

  public synchronized void addToFailedServers(InetSocketAddress address, Throwable t) {
    final long expiry = EnvironmentEdgeManager.currentTimeMillis() + recheckServersTimeout;
    failedServers
        .addFirst(new FailedServer(expiry, address.toString(), t, expiry - recheckServersTimeout));
  }

  /**
   * Check if the server should be considered as bad. Clean the old entries of the list.
   * @return true if the server is in the failed servers list
   */
  public synchronized FailedServer getFailedServer(final InetSocketAddress address) {
    if (failedServers.isEmpty()) {
      return null;
    }

    final String lookup = address.toString();
    final long now = EnvironmentEdgeManager.currentTimeMillis();

    // iterate, looking for the search entry and cleaning expired entries
    Iterator<FailedServer> it = failedServers.iterator();
    while (it.hasNext()) {
      FailedServer cur = it.next();
      if (cur.expiry < now) {
        it.remove();
      } else {
        if (lookup.equals(cur.address)) {
          return cur;
        }
      }
    }

    return null;
  }
}

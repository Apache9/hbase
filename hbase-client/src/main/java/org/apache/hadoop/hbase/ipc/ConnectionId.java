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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.User;

/**
 * This class holds the address and the user ticket, etc. The client connections to servers are
 * uniquely identified by &lt;remoteAddress, ticket, serviceName, rpcTimeout&gt;
 */
@InterfaceAudience.Private
class ConnectionId {
  final InetSocketAddress address;
  final User ticket;
  final int rpcTimeout;
  private static final int PRIME = 16777619;
  final String serviceName;

  ConnectionId(User ticket, String serviceName, InetSocketAddress address, int rpcTimeout) {
    this.address = address;
    this.ticket = ticket;
    this.rpcTimeout = rpcTimeout;
    this.serviceName = serviceName;
  }

  String getServiceName() {
    return this.serviceName;
  }

  InetSocketAddress getAddress() {
    return address;
  }

  User getTicket() {
    return ticket;
  }

  @Override
  public String toString() {
    return this.address.toString() + "/" + this.serviceName + "/" + this.ticket + "/"
        + this.rpcTimeout;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ConnectionId) {
      ConnectionId id = (ConnectionId) obj;
      return address.equals(id.address)
          && ((ticket != null && ticket.equals(id.ticket)) || (ticket == id.ticket))
          && rpcTimeout == id.rpcTimeout && this.serviceName == id.serviceName;
    }
    return false;
  }

  @Override // simply use the default Object#hashcode() ?
  public int hashCode() {
    int hashcode = (address.hashCode()
        + PRIME * (PRIME * this.serviceName.hashCode() ^ (ticket == null ? 0 : ticket.hashCode())))
        ^ rpcTimeout;
    return hashcode;
  }
}
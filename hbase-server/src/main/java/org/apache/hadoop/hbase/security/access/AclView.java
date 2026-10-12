/*
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
package org.apache.hadoop.hbase.security.access;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * The read side of the process side ACL state: hands out the current {@link AclSnapshot} for
 * {@link AuthManager} to make authorization decisions on. Implemented by the process side ACL
 * holder, {@link AclManager} on the master and {@link AclStore} on a region server, which own the
 * snapshot and publish a new one on every change; readers pin one snapshot with {@link #current()}
 * and never block.
 */
@InterfaceAudience.Private
public interface AclView {

  /**
   * The current snapshot. Pin one at the start of an authorization decision and use it for the
   * whole decision, so the decision never observes a half applied update.
   */
  AclSnapshot current();
}

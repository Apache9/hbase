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
package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Carry things that we want to pass to the consume task in event loop. Only one field can be
 * non-null.
 * <p>
 * TODO: need to unify this and {@link FSHLogRingBufferTruck}. There are mostly the same thing.
 */
@InterfaceAudience.Private
final class AsyncFSWALRingBufferTruck {

  public enum Type {
    APPEND, SYNC, EMPTY
  }

  private Type type = Type.EMPTY;

  // a wal entry which need to be appended
  private FSWALEntry entry;

  // indicate that we need to sync our wal writer.
  private SyncFuture sync;

  void load(FSWALEntry entry) {
    this.entry = entry;
    this.type = Type.APPEND;
  }

  void load(SyncFuture sync) {
    this.sync = sync;
    this.type = Type.SYNC;
  }

  Type type() {
    return type;
  }

  FSWALEntry unloadAppend() {
    FSWALEntry entry = this.entry;
    this.entry = null;
    this.type = Type.EMPTY;
    return entry;
  }

  SyncFuture unloadSync() {
    SyncFuture sync = this.sync;
    this.sync = null;
    this.type = Type.EMPTY;
    return sync;
  }

  @Override
  public String toString() {
    return "AsyncFSWALRingBufferTruck [type=" + type + ", entry=" + entry + ", sync=" + sync + "]";
  }
}

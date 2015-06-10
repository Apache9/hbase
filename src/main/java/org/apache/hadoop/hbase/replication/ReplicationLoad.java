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
package org.apache.hadoop.hbase.replication;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class ReplicationLoad implements Writable {
  private static final byte VERSION = 0;
  private String peerId;
  private int sizeOfLogQueue;

  public ReplicationLoad() {
  }

  public ReplicationLoad(String peerId, int sizeOfLogQueue) {
    super();
    this.peerId = peerId;
    this.sizeOfLogQueue = sizeOfLogQueue;
  }

  public String getPeerId() {
    return peerId;
  }

  public void setPeerId(String peerId) {
    this.peerId = peerId;
  }


  public int getSizeOfLogQueue() {
    return sizeOfLogQueue;
  }

  public void setSizeOfLogQueue(int sizeOfLogQueue) {
    this.sizeOfLogQueue = sizeOfLogQueue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(VERSION);
    WritableUtils.writeString(out, peerId);
    out.writeInt(sizeOfLogQueue);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int version = in.readByte();
    if (version > VERSION) throw new IOException("Version mismatch; " + version);
    peerId = WritableUtils.readString(in);
    sizeOfLogQueue = in.readInt();
  }
}

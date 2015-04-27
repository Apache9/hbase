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

package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;

public class CompactionQuota implements Writable {
  private static final byte VERSION = 0;

  // compaction quota the regionserver is using
  private int usingQuota = 0;
  // compaction quota the regionserver requests
  private int requestQuota = 0;
  // compaction quota the master grants to the regionserver
  private int grantQuota = 0;

  // for writable
  public CompactionQuota() {
    
  }
  
  public CompactionQuota(CompactionQuota other) {
    this.usingQuota = other.usingQuota;
    this.requestQuota = other.requestQuota;
    this.grantQuota = other.grantQuota;
  }
  
  public CompactionQuota(int usingQuota, int requestQuota, int grantQuota) {
    this.usingQuota = usingQuota;
    this.requestQuota = requestQuota;
    this.grantQuota = grantQuota;
  }


  public int getUsingQuota() {
    return usingQuota;
  }

  public void setUsingQuota(int usingQuota) {
    this.usingQuota = usingQuota;
  }

  public int getRequestQuota() {
    return requestQuota;
  }

  public void setRequestQuota(int requestQuota) {
    this.requestQuota = requestQuota;
  }

  public int getGrantQuota() {
    return grantQuota;
  }

  public void setGrantQuota(int grantQuota) {
    this.grantQuota = grantQuota;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    if (version != VERSION) {
      throw new VersionMismatchException(VERSION, version);
    }
    this.usingQuota = in.readInt();
    this.requestQuota = in.readInt();
    this.grantQuota = in.readInt();
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.write(VERSION);
    out.writeInt(usingQuota);
    out.writeInt(requestQuota);
    out.writeInt(grantQuota);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CompactionQuota {");
    sb.append(" usingQuota: " + usingQuota);
    sb.append(" requestQuota: " + requestQuota);
    sb.append(" grantQuota: " + grantQuota);
    sb.append("}");
    return sb.toString();
  }
}

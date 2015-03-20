/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.hbase.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.regionserver.CompactionQuota;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;

/**
 * RegionServer report Request
 */

public class RSReportRequest implements Writable {
  private static final byte VERSION = 0;

  private HServerLoad serverLoad;
  private CompactionQuota compactionQuota;

  // for writable
  public RSReportRequest() {
    
  }
  public RSReportRequest(HServerLoad serverLoad,
      CompactionQuota compactionQuota) {
    super();
    this.serverLoad = serverLoad;
    this.compactionQuota = compactionQuota;
  }

  public HServerLoad getServerLoad() {
    return serverLoad;
  }

  public CompactionQuota getCompactionQuotaRequest() {
    return compactionQuota;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(VERSION);
    serverLoad.write(out);
    compactionQuota.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    if (version != VERSION) {
      throw new VersionMismatchException(VERSION, version);
    }
    serverLoad = new HServerLoad();
    serverLoad.readFields(in);
    compactionQuota = new CompactionQuota();
    compactionQuota.readFields(in);
  }
}
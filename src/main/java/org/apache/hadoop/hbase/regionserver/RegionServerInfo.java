/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.io.Writable;

/**
 * Region server's static info like info port. These info will be written on
 * region server's ephemeral zookeeper node at the startup for Hmaster to watch.
 */

public class RegionServerInfo implements Writable {
  private static final short VERSION = 0;

  private int infoPort = 0;

  public RegionServerInfo() {
  }

  public int getInfoPort() {
    return infoPort;
  }

  public void setInfoPort(int infoPort) {
    this.infoPort = infoPort;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int version = in.readInt();
    if (version != VERSION) {
      throw new IOException("unsupported version: " + version);
    }
    this.infoPort = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION);
    out.writeInt(infoPort);
  }
}

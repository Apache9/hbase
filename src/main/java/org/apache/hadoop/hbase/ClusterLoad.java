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

package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.util.Strings;

/**
 * This class is used exporting current state of load on the cluster.
 */

public class ClusterLoad {
  private int tableNum;
  private int regionServerNum;
  private int regionNum;
  private long readRequestPerSecond;
  private long writeRequestPerSecond;
  private long requestPerSecond;

  public ClusterLoad(int tableNum, int regionServerNum, int regionNum,
      long readRequestPerSecond, long writeRequestPerSecond) {
    super();
    this.tableNum = tableNum;
    this.regionServerNum = regionServerNum;
    this.regionNum = regionNum;
    this.readRequestPerSecond = readRequestPerSecond;
    this.writeRequestPerSecond = writeRequestPerSecond;
    this.requestPerSecond = writeRequestPerSecond + writeRequestPerSecond;
  }

  public int getTableNum() {
    return tableNum;
  }

  public int getRegionServerNum() {
    return regionServerNum;
  }

  public int getRegionNum() {
    return regionNum;
  }

  public long getReadRequestPerSecond() {
    return readRequestPerSecond;
  }

  public long getWriteRequestPerSecond() {
    return writeRequestPerSecond;
  }

  public long getRequestPerSecond() {
    return requestPerSecond;
  }

  @Override
  public String toString() {
    StringBuilder sb =
        Strings.appendKeyValue(new StringBuilder(), "numberOfTable:",
          Integer.valueOf(tableNum));
    sb =
        Strings.appendKeyValue(sb, "numberOfRegionServer",
          Integer.valueOf(this.regionServerNum));
    sb =
        Strings.appendKeyValue(sb, "numberOfRegion",
          Integer.valueOf(this.regionNum));
    sb =
        Strings.appendKeyValue(sb, "readRequestPerSecond",
          Long.valueOf(this.readRequestPerSecond));
    sb =
        Strings.appendKeyValue(sb, "writeRequestPerSecond",
          Long.valueOf(this.writeRequestPerSecond));
    sb =
        Strings.appendKeyValue(sb, "requestPerSecond",
          Long.valueOf(this.requestPerSecond));
    return sb.toString();
  }
}

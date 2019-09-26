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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsClusterSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Aggregation";

  /**
   * The context metrics will be under.
   */
  String METRICS_CONTEXT = "cluster";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Overview metrics about HBase cluster.";

  // Xiaomi metrics, keep name compatible with 0.98
  String READ_REQUEST_PER_SECOND = "readRequestsPerSecond";
  String READ_REQUEST_PER_SECOND_DESC = "Number of read requests per second";
  String WRITE_REQUEST_PER_SECOND = "writeRequestsPerSecond";
  String WRITE_REQUEST_PER_SECOND_DESC = "Number of write requests per second";
  String GET_REQEUST_PER_SECOND = "getRequestsPerSecond";
  String GET_REQUEST_PER_SECOND_DESC = "Number of get requests per second";
  String SCAN_REQUEST_PER_SECOND = "scanRequestsCountPerSecond";
  String SCAN_REQUEST_PER_SECOND_DESC = "Number of scan requests per second";
  String SCAN_ROWS_COUNT_PER_SECOND = "scanRowsCountPerSecond";
  String SCAN_ROWS_COUNT_PER_SECOND_DESC = "Number of scan request rows per second";
  String READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND = "readRequestsByCapacityUnitPerSecond";
  String READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC = "Read bytes per second, unit: 1KB";
  String WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND = "writeRequestsByCapacityUnitPerSecond";
  String WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC = "Write bytes per second, unit: 1KB";
  String READ_CELLS_PER_SECOND = "readCellCountPerSecond";
  String READ_CELLS_PER_SECOND_DESC = "Number of read cells per second";
  String READ_RAW_CELLS_PER_SECOND = "readRawCellCountPerSecond";
  String READ_RAW_CELLS_PER_SECOND_DESC = "Number of raw cell read per second";
  String REGION_COUNT = "regionCount";
  String REGION_COUNT_DESC = "Number of regions";
  String MEMSTORE_SIZE_MB = "memstoreSizeMB";
  String MEMSTORE_SIZE_MB_DESC = "Size of the memstore";
  String STOREFILE_SIZE_MB = "storefileSizeMB";
  String STOREFILE_SIZE_MB_DESC = "Size of the storefiles";
  String TABLE_COUNT = "tableCount";
  String TABLE_COUNT_DESC = "Number of tables";
  String NAMESPACE_COUNT = "namespaceCount";
  String NAMESPACE_COUNT_DESC = "Number of namespaces";
  String SIZE_OF_LOG_QUEUE = "sizeOfLogQueue";
  String SIZE_OF_LOG_QUEUE_DESC = "Number of replication source queue logs";
  String REPLICATION_LAG = "replicationLag";
  String REPLICATION_LAG_DESC = "Replication lag";
  String APPROXIMATE_ROW_COUNT = "approximateRowCount";
  String APPROXIMATE_ROW_COUNT_DESC = "Number of approximate row count";
}

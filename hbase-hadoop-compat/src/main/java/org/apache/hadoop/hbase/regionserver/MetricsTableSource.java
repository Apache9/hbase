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

package org.apache.hadoop.hbase.regionserver;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This interface will be implemented to allow region server to push table metrics into
 * MetricsRegionAggregateSource that will in turn push data to the Hadoop metrics system.
 */
@InterfaceAudience.Private
public interface MetricsTableSource extends Comparable<MetricsTableSource> {

  String READ_REQUEST_COUNT = "readRequestCount";
  String READ_REQUEST_COUNT_DESC = "Number of read requests";
  String WRITE_REQUEST_COUNT = "writeRequestCount";
  String WRITE_REQUEST_COUNT_DESC = "Number of write requests";
  String TOTAL_REQUEST_COUNT = "totalRequestCount";
  String TOTAL_REQUEST_COUNT_DESC = "Number of total requests";
  String MEMSTORE_SIZE = "memstoreSize";
  String MEMSTORE_SIZE_DESC = "The size of memory stores";
  String STORE_FILE_SIZE = "storeFileSize";
  String STORE_FILE_SIZE_DESC = "The size of store files size";
  String TABLE_SIZE = "tableSize";
  String TABLE_SIZE_DESC = "Total size of the table in the region server";

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

  String getTableName();

  /**
   * Close the table's metrics as all the region are closing.
   */
  void close();

  /**
   * Get the aggregate source to which this reports.
   */
  MetricsTableAggregateSource getAggregateSource();

}

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

// NOTE: The "required" and "optional" keywords for the service methods are purely for documentation

namespace java org.apache.hadoop.hbase.replication.thrift.generated

struct TClusterId {
    1: i64 lb,
    2: i64 ub
}

/**
 * Represents a single cell and its value.
 */
struct TColumnValue {
  1: required binary row
  2: required binary family,
  3: required binary qualifier,
  4: required binary value,
  5: optional i64 timestamp,
  6: required byte type
}

/**
 * Mapping for HLogKey
 *
*/
struct THLogKey {
    1: required binary tableName,
    2: required i64 writeTime,
    3: required i64 seqNum
}

/**
 * Mapping for WALEdit
 *
*/
struct TWalLEdit {
    1: required list<TColumnValue> mutations,
}

struct TEdit {
  1: required THLogKey hLogKey,
  2: required TWalLEdit walEdit
  3: required list<TClusterId> clusterIds
}

struct TBatchEdit {
  1: required list<TEdit> edits
}

//
// Exceptions
//

/**
 * A TIOError exception signals that an error occurred communicating
 * to the HBase master or a HBase region server. Also used to return
 * more general HBase error conditions.
 */
exception TIOError {
  1: optional string message
}

/**
 * A TIllegalArgument exception indicates an illegal or invalid
 * argument was passed into a procedure.
 */
exception TIllegalArgument {
  1: optional string message
}

service THBaseService {

  void replicate(
    1: required TBatchEdit edits ) throws (1: TIOError io)

  void ping()

  string getClusterUUID()
}

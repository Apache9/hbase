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
package org.apache.hadoop.hbase.mapreduce.salted;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TestTableInputFormatScanBase;
import org.junit.BeforeClass;

public abstract class TestSaltedTableInputFormatScanBase extends TestTableInputFormatScanBase {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // test intermittently fails under hadoop2 (2.0.2-alpha) if shortcircuit-read (scr) is on.
    // this turns it off for this test. TODO: Figure out why scr breaks recovery.
    System.setProperty("hbase.tests.use.shortcircuit.reads", "false");

    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(TableInputFormat.class);
    TEST_UTIL.enableDebug(TableInputFormatBase.class);
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    table = TEST_UTIL.createSaltedTable(TABLE_NAME, INPUT_FAMILYS, 26);
    TEST_UTIL.loadTable(table, INPUT_FAMILYS, null, false);
  }
}

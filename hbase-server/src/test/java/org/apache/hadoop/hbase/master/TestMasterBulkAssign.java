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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.util.List;

@Category(MediumTests.class)
public class TestMasterBulkAssign {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestMasterBulkAssign.class);
  private static final HRegionInfo mockRegionInfo = new HRegionInfo(TableName.valueOf("mockTableName"),
      Bytes.toBytes("mock_key1"), Bytes.toBytes("mock_key2"));
  private static RegionStates mockRegionStates = Mockito.mock(RegionStates.class);
  private static AssignmentManager mockAssignmentManager = Mockito.mock(AssignmentManager.class);
  private static HMaster master;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.startMiniCluster();
    Mockito.when(mockRegionStates.getRegionInfo(mockRegionInfo.getEncodedNameAsBytes())).thenReturn(mockRegionInfo);
    Mockito.when(mockAssignmentManager.getRegionStates()).thenReturn(mockRegionStates);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    master.assignmentManager = mockAssignmentManager;
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBulkAssignRegion() throws Exception {
    master.bulkAssignRegion(null, RequestConverter.buildBulkAssignRegionRequest(Lists.newArrayList(mockRegionInfo)));
    verify(mockAssignmentManager, times(1)).assign(Mockito.any(List.class));
  }
}


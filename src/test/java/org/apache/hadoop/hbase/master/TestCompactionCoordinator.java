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

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.CompactionQuota;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestCompactionCoordinator {

  @Test
  public void testResquestQuota() {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(CompactionCoordinator.HBASE_CLUSTER_COMPACTION_RATIO, 10.0f);
    
    ServerManager manager = Mockito.mock(ServerManager.class);
    Mockito.when(manager.countOfRegionServers()).thenReturn(2);
    CompactionCoordinator coordinator =
        new CompactionCoordinator(conf, manager);
    Assert.assertEquals(20, coordinator.getTotalQuota());    

    ServerName rs1 = new ServerName("localhost", 123, 123L);
    CompactionQuota request = new CompactionQuota(0, 11, 0);
    CompactionQuota response = coordinator.requestCompactionQuota(rs1, request);
    Assert.assertEquals(11, response.getGrantQuota());
    Assert.assertEquals(11, coordinator.getUsedQuota());
    
    ServerName rs2 = new ServerName("localhost", 124, 124L);
    request = new CompactionQuota(0, 10, 0);
    response = coordinator.requestCompactionQuota(rs2, request);
    Assert.assertEquals(9, response.getGrantQuota());
    Assert.assertEquals(20, coordinator.getUsedQuota());
  }

  @Test
  public void testOverQuota() {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(CompactionCoordinator.HBASE_CLUSTER_COMPACTION_RATIO, 10.0f);
    ServerManager manager = Mockito.mock(ServerManager.class);
    Mockito.when(manager.countOfRegionServers()).thenReturn(1);
    CompactionCoordinator coordinator =
        new CompactionCoordinator(conf, manager);
    Assert.assertEquals(10, coordinator.getTotalQuota());    

    ServerName rs1 = new ServerName("localhost", 123, 123L);
    CompactionQuota request = new CompactionQuota(0, 10, 0);
    CompactionQuota response = coordinator.requestCompactionQuota(rs1, request);
    Assert.assertEquals(10, response.getGrantQuota());
   
    request = new CompactionQuota(10, 10, 0);
    response = coordinator.requestCompactionQuota(rs1, request);
    Assert.assertEquals(0, response.getGrantQuota());
  }

  @Test
  public void testUsedQuota() {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(CompactionCoordinator.HBASE_CLUSTER_COMPACTION_RATIO, 10.0f);

    ServerManager manager = Mockito.mock(ServerManager.class);
    Mockito.when(manager.countOfRegionServers()).thenReturn(2);
    CompactionCoordinator coordinator =
        new CompactionCoordinator(conf, manager);
    Assert.assertEquals(20, coordinator.getTotalQuota());

    ServerName rs = new ServerName("localhost", 123, 123L);
    CompactionQuota request = new CompactionQuota(0, 11, 0);
    CompactionQuota response = coordinator.requestCompactionQuota(rs, request);
    Assert.assertEquals(11, response.getGrantQuota());
    Assert.assertEquals(11, coordinator.getUsedQuota());

    request = new CompactionQuota(9, 0, 0);
    response = coordinator.requestCompactionQuota(rs, request);
    Assert.assertEquals(0, response.getGrantQuota());
    Assert.assertEquals(9, coordinator.getUsedQuota());

    request = new CompactionQuota(0, 0, 0);
    response = coordinator.requestCompactionQuota(rs, request);
    Assert.assertEquals(0, response.getGrantQuota());
    Assert.assertEquals(0, coordinator.getUsedQuota());
  }
}

/**
 * Copyright 2013 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests of opening HTable using name service.
 */
@Category(SmallTests.class)
public class TestHTableNameService {
  private final static Configuration conf = HBaseConfiguration.create();
  
  private void securityDisabled(HTable hTable) {
    Assert.assertEquals("simple", hTable.getConfiguration().get("hbase.security.authentication"));
    Assert.assertEquals("authentication",
      hTable.getConfiguration().get("hadoop.rpc.protection"));
    Assert.assertNull(hTable.getConfiguration().get("hbase.rpc.protection"));
  }

  private void securityEnabled(HTable hTable) {
    Assert.assertEquals("kerberos",
      hTable.getConfiguration().get("hadoop.security.authentication"));
    Assert.assertEquals("kerberos",
      hTable.getConfiguration().get("hbase.security.authentication"));
    Assert.assertEquals("hbase_tst/hadoop@XIAOMI.HADOOP",
      hTable.getConfiguration().get("hbase.master.kerberos.principal"));
    Assert.assertEquals("hbase_tst/hadoop@XIAOMI.HADOOP",
      hTable.getConfiguration().get("hbase.regionserver.kerberos.principal"));
    Assert.assertEquals("org.apache.hadoop.hbase.ipc.SecureRpcEngine",
      hTable.getConfiguration().get("hbase.rpc.engine"));
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf.set("hadoop.security.authentication", "kerberos");
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    conf.unset("hadoop.security.authentication");
  }

  
  @Test
  public void testShortTableName() throws IOException {
    HTable hTable = new HTable(conf, "test_table1", false);

    Assert.assertArrayEquals(Bytes.toBytes("test_table1"), hTable.getTableName());

    Assert.assertEquals("localhost",
      hTable.getConfiguration().get("hbase.zookeeper.quorum"));
    Assert.assertEquals("/hbase",
      hTable.getConfiguration().get("zookeeper.znode.parent"));
    Assert.assertEquals(2181,
      hTable.getConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));

    securityDisabled(hTable);

    hTable.close();
  }

  @Test
  public void testFullUri() throws IOException {
    HTable hTable = new HTable(conf, "hbase://tjwq02tst-staging/test_table1", false);

    Assert.assertArrayEquals(Bytes.toBytes("test_table1"), hTable.getTableName());

    Assert.assertEquals("10.38.162.228,10.38.162.36,10.38.166.11,10.38.166.12,10.38.166.20",
      hTable.getConfiguration().get("hbase.zookeeper.quorum"));
    Assert.assertEquals("/hbase/tjwq02tst-staging",
      hTable.getConfiguration().get("zookeeper.znode.parent"));
    Assert.assertEquals(11000,
      hTable.getConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));

    securityEnabled(hTable);

    hTable.close();
  }

  @Test
  public void testFullUriWithPort() throws IOException {
    HTable hTable = new HTable(conf, "hbase://tjwq02tst-staging:11000/test_table1", false);

    Assert.assertArrayEquals(Bytes.toBytes("test_table1"), hTable.getTableName());

    Assert.assertEquals("10.38.162.228,10.38.162.36,10.38.166.11,10.38.166.12,10.38.166.20",
      hTable.getConfiguration().get("hbase.zookeeper.quorum"));
    Assert.assertEquals("/hbase/tjwq02tst-staging",
      hTable.getConfiguration().get("zookeeper.znode.parent"));
    Assert.assertEquals(11000,
      hTable.getConfiguration().getInt("hbase.zookeeper.property.clientPort", -1));

    securityEnabled(hTable);

    hTable.close();
  }

  @Test
  public void testFullUriWithoutTableName() throws IOException {
    try {
      HTable hTable = new HTable(conf, "hbase://hytst-staging98:9800/", false);
      // TableName not allowed empty qualifier
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testIllegalUri() throws IOException {
    try {
      new HTable(conf, "http://test/test", false);
      Assert.fail("Exception was expected!");
    } catch (IOException e) {
      Assert.assertEquals("Illegal zookeeper cluster type: est", e.getMessage());
    }

    try {
      new HTable(conf, "hdfs://xmdmtst-test/test", false);
      Assert.fail("Exception was expected!");
    } catch (IOException e) {
      Assert.assertEquals("Unrecognized scheme: hdfs", e.getMessage());
    }

    try {
      new HTable(conf, "://test/test", false);
      Assert.fail("Exception was expected!");
    } catch (IOException e) {
      Assert.assertEquals("Illegal uri: Expected scheme name at index 0: ://test/test",
        e.getMessage());
    }

    try {
      new HTable(conf, "hbase://test/test", false);
      Assert.fail("Exception was expected!");
    } catch (IOException e) {
      Assert.assertEquals("Illegal zookeeper cluster type: est", e.getMessage());
    }

    try {
      new HTable(conf, "hbase://xmdmtst/test", false);
      Assert.fail("Exception was expected!");
    } catch (IOException e) {
      Assert.assertEquals("Illegal scheme, 'zk' expected: hbase", e.getMessage());
    }

    try {
      new HTable(conf, "hbase://xmdmttt-test/test", false);
      Assert.fail("Exception was expected!");
    } catch (IOException e) {
      Assert.assertEquals("Illegal zookeeper cluster type: ttt", e.getMessage());
    }

    try {
      new HTable(conf, "hbase://xmdmttt-test:9800/test", false);
      Assert.fail("Exception was expected!");
    } catch (IOException e) {
      Assert.assertEquals("Illegal zookeeper cluster type: ttt", e.getMessage());
    }
  }

//  @org.junit.Rule
//  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
//    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

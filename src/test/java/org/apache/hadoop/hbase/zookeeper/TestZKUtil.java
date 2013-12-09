/**
 * Copyright 2010 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership. The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;

@Category(SmallTests.class)
public class TestZKUtil {

  @Test
  public void testCreateACL() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "TestZKUtil", null, false);
    
    ArrayList<ACL> acls = ZKUtil.createACL(zkw, "/hdfs");
    assertEquals(acls, Ids.OPEN_ACL_UNSAFE);
    
    acls = ZKUtil.createACL(zkw, zkw.baseZNode);
    assertEquals(acls, Ids.OPEN_ACL_UNSAFE);

    acls = ZKUtil.createACL(zkw, zkw.clusterIdZNode);
    assertEquals(acls, Ids.OPEN_ACL_UNSAFE);
  }

  @Test
  public void testCreateACLInSecureCluster() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // secure cluster
    conf.set("hbase.security.authentication", "kerberos");
    conf.set("hbase.zookeeper.client.keytab.file", "/tmp/hbase.keytab");

    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "TestZKUtil", null, false);
    String adminName = zkw.getConfiguration().get("hbase.superuser", "hbase_admin");
    ACL admin = new ACL(Perms.ALL, new Id("sasl", adminName));

    ArrayList<ACL> acls = ZKUtil.createACL(zkw, "/hdfs");
    assertEquals(acls, Ids.OPEN_ACL_UNSAFE);
    
    acls = ZKUtil.createACL(zkw, zkw.baseZNode);
    assertEquals(3, acls.size());
    assertEquals(ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE,
      acls.subList(0, 2));
    assertEquals(admin, acls.get(2));
    
    acls = ZKUtil.createACL(zkw, zkw.clusterIdZNode);
    assertEquals(3, acls.size());
    assertEquals(ZooKeeperWatcher.CREATOR_ALL_AND_WORLD_READABLE,
      acls.subList(0, 2));
    assertEquals(admin, acls.get(2));
  }
}

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
package com.xiaomi.infra.hbase.util;

import static com.xiaomi.infra.hbase.util.CanaryPerfCounterUtils.HOSTNAME;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.xiaomi.infra.hbase.util.CanaryPerfCounterUtils.PerfCounterNameJoiner;

@Category(SmallTests.class)
public class TestCanaryPerfCounterUtils {

  @Test
  public void testPerfCounterNameJoiner() {
    String name = new PerfCounterNameJoiner().method("read").cluster("cluster")
        .table(TableName.valueOf("table")).getName();
    assertEquals("hbase-canary-read,cluster=cluster,table=table", name);

    name = new PerfCounterNameJoiner().method("read").cluster("cluster")
        .table(TableName.valueOf("table")).host().getName();
    assertEquals("hbase-canary-read,cluster=cluster,table=table,host=" + HOSTNAME, name);

    name = new PerfCounterNameJoiner().method("read").cluster("hbase://cluster/")
        .table(TableName.valueOf("ns:table"))
        .getName();
    assertEquals("hbase-canary-read,cluster=cluster,table=ns:table", name);
  }

}
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
package org.apache.hadoop.hbase.replication.thrift;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestThriftReplicationBase {

  @BeforeClass
  public static void setUpKlazz() throws Exception {
    // Error level to skip some warnings specific to the minicluster. See HBASE-4709
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.util.MBeans.class).setLevel( org.apache.log4j.Level.ERROR);
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class).setLevel(org.apache.log4j.Level.ERROR);

  }
}

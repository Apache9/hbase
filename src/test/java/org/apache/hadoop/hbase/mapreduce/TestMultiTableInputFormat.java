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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.LargeTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests various scan start and stop row scenarios. This is set in a scan and
 * tested in a MapReduce job to see if that is handed over and done properly
 * too.
 */
@Category(LargeTests.class)
public class TestMultiTableInputFormat extends TestMultiTableInputFormatBase {
  static final Log LOG = LogFactory.getLog(TestMultiTableInputFormat.class);

  @Test
  public void testScanEmptyToEmpty() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, null, null);
  }
  
  @Test
  public void testScanEmptyToAPP() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, "app", "apo");
  }

  @Test
  public void testScanOBBToOPP() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan("obb", "opp", "opo");
  }

  @Test
  public void testScanOPPToEmpty() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan("opp", null, "zzz");
  }

  @Test
  public void testScanYZYToEmpty() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan("yzy", null, "zzz");
  }
}

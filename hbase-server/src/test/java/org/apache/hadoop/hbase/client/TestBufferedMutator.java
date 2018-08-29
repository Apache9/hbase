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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

@Category({ MediumTests.class })
public class TestBufferedMutator {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("test");

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static int COUNT = 1024;

  private static byte[] VALUE = new byte[1024];

  private static AsyncConnection CONN;

  private static final int TIMEOUT = 60000;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, CF);
    CONN = HConnectionManager.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    ThreadLocalRandom.current().nextBytes(VALUE);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    BufferedMutator mutator =
        new BufferedMutatorBuilderImpl(CONN.getBufferedMutatorBuilder(TABLE_NAME)).build();
    mutator.mutate(
        IntStream.range(0, COUNT / 2).mapToObj(i -> new Put(Bytes.toBytes(i)).add(CF, CQ, VALUE))
            .collect(Collectors.toList()));
    mutator.flush();
    mutator.mutate(
        IntStream.range(COUNT / 2, COUNT).mapToObj(i -> new Put(Bytes.toBytes(i)).add(CF, CQ, VALUE))
            .collect(Collectors.toList()));
    mutator.close();

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() < startTime + TIMEOUT) {
      if (((BufferedMutatorImpl) mutator).getBufferedFutruesNumber() == 0) {
        verifyData();
        return;
      }
      Thread.sleep(1000);
    }
    fail("Wait too much time for mutator to finish all mutations");
  }

  private void verifyData() {
    AsyncTable table = CONN.getTable(TABLE_NAME);
    IntStream.range(0, COUNT).mapToObj(i -> new Get(Bytes.toBytes(i))).map(g -> table.get(g).join())
        .forEach(r -> {
          assertArrayEquals(VALUE, ((Result) r).getValue(CF, CQ));
        });
  }
}

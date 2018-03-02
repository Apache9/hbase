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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestSerialReplication {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static Path LOG_DIR;

  private static ProtobufLogWriter WRITER;

  public static final class LocalReplicationEndpoint extends BaseReplicationEndpoint {

    private static final UUID PEER_UUID = UUID.randomUUID();

    @Override
    public UUID getPeerUUID() {
      return PEER_UUID;
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      for (Entry entry : replicateContext.getEntries()) {
        try {
          WRITER.append(entry);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return false;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(3);
    LOG_DIR = UTIL.getDataTestDirOnTestFS("replicated");
    UTIL.getTestFileSystem().mkdirs(LOG_DIR);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Rule
  public final TestName name = new TestName();

  private Path logPath;

  @Before
  public void setUp() throws IOException, StreamLacksCapabilityException {
    UTIL.ensureSomeRegionServersAvailable(3);
    logPath = new Path(LOG_DIR, name.getMethodName());
    WRITER = new ProtobufLogWriter();
    WRITER.init(UTIL.getTestFileSystem(), logPath, UTIL.getConfiguration(), false);
  }

  @After
  public void tearDown() throws IOException {
    if (WRITER != null) {
      WRITER.close();
      WRITER = null;
    }
  }

  @Test
  public void testRegionMove() {
    
  }

  @Test
  public void testFailover() {
    
  }

  @Test
  public void testSplit() {
    
  }

  @Test
  public void testMerge() {
    
  }
}

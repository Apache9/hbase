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
package org.apache.hadoop.hbase.procedure2.store.region;

import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreBase;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A procedure store which uses a region to store all the procedures.
 */
@InterfaceAudience.Private
public class RegionProcedureStore extends ProcedureStoreBase {

  private static final Logger LOG = LoggerFactory.getLogger(RegionProcedureStore.class);

  private static final TableName TABLE_NAME = TableName.valueOf("hbase:procedure");

  private static final byte[] FAMILY = Bytes.toBytes("info");

  private static final TableDescriptor TABLE_DESC = TableDescriptorBuilder.newBuilder(TABLE_NAME)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();

  private WALProvider provider;

  private WAL wal;

  private Region region;

  public RegionProcedureStore() {

  }

  @Override
  public void start(int numThreads) throws IOException {

  }

  @Override
  public void stop(boolean abort) {
  }

  @Override
  public int getNumThreads() {
    // TODO Implement ProcedureStore.getNumThreads
    return 0;
  }

  @Override
  public int setRunningProcedureCount(int count) {
    // TODO Implement ProcedureStore.setRunningProcedureCount
    return 0;
  }

  @Override
  public void recoverLease() throws IOException {
    // TODO Implement ProcedureStore.recoverLease

  }

  @Override
  public void load(ProcedureLoader loader) throws IOException {
    // TODO Implement ProcedureStore.load

  }

  @Override
  public void insert(Procedure<?> proc, Procedure<?>[] subprocs) {
    // TODO Implement ProcedureStore.insert

  }

  @Override
  public void insert(Procedure<?>[] procs) {
    // TODO Implement ProcedureStore.insert

  }

  @Override
  public void update(Procedure<?> proc) {
    // TODO Implement ProcedureStore.update

  }

  @Override
  public void delete(long procId) {
    // TODO Implement ProcedureStore.delete

  }

  @Override
  public void delete(Procedure<?> parentProc, long[] subProcIds) {
    // TODO Implement ProcedureStore.delete

  }

  @Override
  public void delete(long[] procIds, int offset, int count) {
    // TODO Implement ProcedureStore.delete

  }

}

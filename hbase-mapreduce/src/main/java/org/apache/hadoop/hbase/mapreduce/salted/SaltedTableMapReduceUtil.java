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
package org.apache.hadoop.hbase.mapreduce.salted;

import com.xiaomi.infra.base.nameservice.NameService;
import com.xiaomi.infra.hbase.salted.SaltedHTable;
import com.xiaomi.infra.hbase.salted.SaltedHTable.SlotsWritable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
public class SaltedTableMapReduceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SaltedTableMapReduceUtil.class);
  
  public static List<InputSplit> getSplitsForSaltedTable(String fullTableName, Table table, RegionLocator regionLocator,
      Scan scan) throws IOException {
    byte[][] slots = null;
    // user passed slots
    byte[] slotsValue = scan.getAttribute(SaltedHTable.SLOTS_IN_SCAN);
    if (slotsValue == null) {
      slots = SaltedHTable.getKeySalter(table).getAllSalts();
    } else {
      SlotsWritable slotsWritable = new SlotsWritable();
      Writables.getWritable(slotsValue, slotsWritable);
      slots = slotsWritable.getSlots();
    }
    List<InputSplit> splits = new ArrayList<InputSplit>(slots.length);
    for (int i = 0; i < slots.length; i++) {
      String regionLocation = regionLocator.getRegionLocation(slots[i]).getHostname();
      // splitStop is meaningless
      InputSplit split = new TableSplit(fullTableName, table.getName(), scan, slots[i],
          HConstants.EMPTY_BYTE_ARRAY, regionLocation, "", 0L);
      splits.add(split);
      if (LOG.isDebugEnabled()) {
        LOG.warn("getSplits for salted Table: split -> " + i + " -> " + split);
      }
    }
    return splits;
  }

  public static RecordReader<ImmutableBytesWritable, Result> createRecordReaderForSaltedTable(
      TableRecordReader reader, Scan scan, InputSplit split, TaskAttemptContext context)
      throws IOException {
    final Connection connection = ConnectionFactory.createConnection(context.getConfiguration(),
      NameService.resolveClusterUri(((TableSplit) split).getFullTableName()));
    Table table = connection.getTable(((TableSplit)split).getTable());
    return createRecordReaderForSaltedTable(connection, table, reader, scan, split, context);
  }

  public static RecordReader<ImmutableBytesWritable, Result> createRecordReaderForSaltedTable(
      Connection connection, Table table, TableRecordReader reader, Scan scan, InputSplit split,
      TaskAttemptContext context) throws IOException {
    if (table == null) {
      throw new IOException("Cannot create a record reader because of a"
          + " previous error. Please look at the previous logs lines from"
          + " the task's full log for more details.");
    }
    TableSplit tSplit = (TableSplit) split;

    // if no table record reader was provided use default
    final TableRecordReader trr = reader == null ? new TableRecordReader() : reader;

    try {
      Scan sc = new Scan(scan);
      SlotsWritable slotsWritable = new SlotsWritable(new byte[][] { tSplit.getStartRow() });
      sc.setAttribute(SaltedHTable.SLOTS_IN_SCAN, Writables.getBytes(slotsWritable));
      trr.setScan(sc);
      trr.setTable(table);
      try {
        trr.initialize(tSplit, context);
      } catch (InterruptedException e) {
        throw new InterruptedIOException(e.getMessage());
      }
      return new TableRecordReader(trr.getRecordReaderImpl()) {
        @Override
        public void close() {
          super.close();
          try {
            connection.close();
          } catch (IOException ioe) {
            LOG.warn("Error closing connection", ioe);
          }
        }
      };
    } catch (IOException ioe) {
      // If there is an exception make sure that all
      // resources are closed and released.
      trr.close();
      connection.close();
      throw ioe;
    }
  }
  
  public static boolean isSaltedTable(Configuration conf, byte[] tableName) throws IOException {
    String tableNameString = Bytes.toString(tableName);
    try (
        Connection conn = ConnectionFactory.createConnection(conf,
          NameService.resolveClusterUri(tableNameString));
        Table table = conn.getTable(NameService.resolveTableName(tableNameString))) {
      TableDescriptor desc = table.getDescriptor();
      return desc.isSalted();
    }
  }
}

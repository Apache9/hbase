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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SaltedMultiTableInputFormat extends MultiTableInputFormat implements
    Configurable {
  private static final Log LOG = LogFactory.getLog(SaltedMultiTableInputFormat.class);
  
  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    TableSplit tSplit = (TableSplit) split;

    if (tSplit.getTableName() == null) {
      throw new IOException("Cannot create a record reader because of a"
          + " previous error. Please look at the previous logs lines from"
          + " the task's full log for more details.");
    }
    
    if (SaltedTableMapReduceUtil.isSaltedTable(context.getConfiguration(), tSplit.getTableName())) {
      return SaltedTableMapReduceUtil.createRecordReaderForSaltedTable(getTableRecordReader(),
        tSplit.getScan(), tSplit, context);
    } else {
      return super.createRecordReader(tSplit, context);
    }
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    LOG.debug("getSplits for salted tables");
    
    if (getScans().isEmpty()) {
      throw new IOException("No scans were provided.");
    }
    List<InputSplit> splits = new ArrayList<InputSplit>();

    for (Scan scan : getScans()) {
      byte[] tableName = scan.getAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME);
      if (tableName == null) 
        throw new IOException("A scan object did not have a table name");
      HTable table = new HTable(context.getConfiguration(), tableName);
      if (table.getTableDescriptor().isSalted()) {
        splits.addAll(SaltedTableMapReduceUtil.getSplitsForSaltedTable(table, scan));
      } else {
        splits.addAll(MultiTableInputFormatBase.getSplitsTable(table, scan));
      }
      table.close();
    }
    return splits;
  }
}

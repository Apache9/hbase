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
import java.util.List;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.RegionSizeCalculator;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SaltedMultiTableInputFormat extends MultiTableInputFormat implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(SaltedMultiTableInputFormat.class);
  
  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    TableSplit tSplit = (TableSplit) split;

    if (tSplit.getFullTableName() == null) {
      throw new IOException("Cannot create a record reader because of a"
          + " previous error. Please look at the previous logs lines from"
          + " the task's full log for more details.");
    }

    if (SaltedTableMapReduceUtil.isSaltedTable(context.getConfiguration(),
      Bytes.toBytes(tSplit.getFullTableName()))) {
      return SaltedTableMapReduceUtil.createRecordReaderForSaltedTable(getTableRecordReader(),
        tSplit.getScan(), tSplit, context);
    } else {
      return super.createRecordReader(tSplit, context);
    }
  }

  @Override
  public List<InputSplit> getSplits(String fullTableName, Table table, Scan scan,
      Pair<byte[][], byte[][]> keys, RegionSizeCalculator sizeCalculator,
      RegionLocator regionLocator) throws IOException {
    LOG.info("getSplits for salted table: " + table.getName().getNameAsString());
    if (table.getDescriptor().isSalted()) {
      return SaltedTableMapReduceUtil.getSplitsForSaltedTable(fullTableName, table, regionLocator,
        scan);
    } else {
      return super.getSplits(fullTableName, table, scan, keys, sizeCalculator, regionLocator);
    }
  }
}

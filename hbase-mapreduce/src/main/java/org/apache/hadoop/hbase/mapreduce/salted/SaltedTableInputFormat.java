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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public class SaltedTableInputFormat extends TableInputFormat {
  private static final Log LOG = LogFactory.getLog(SaltedTableInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    LOG.info("getSplits for salted tables");

    initialize(context);
    Table table = getTable();
    RegionLocator regionLocator = getRegionLocator();
    Scan scan = getScan();
    if (table == null) {
      throw new IOException("No table was provided.");
    }
    
    if (!table.getDescriptor().isSalted()) {
      throw new IOException("table:" + table.getName().getNameAsString()
          + " must be salted table");
    }
    
    return SaltedTableMapReduceUtil.getSplitsForSaltedTable(
      context.getConfiguration().get(INPUT_TABLE), table, regionLocator, scan);
  }
    
  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context)
  throws IOException {
    initialize(context);
    return SaltedTableMapReduceUtil.createRecordReaderForSaltedTable(getTableRecordReader(),
        getScan(), split, context);
  }
}

package org.apache.hadoop.hbase.mapreduce.salted;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class SaltedTableInputFormat extends TableInputFormat {
  private static final Log LOG = LogFactory.getLog(SaltedTableInputFormat.class);
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    LOG.info("getSplits for salted tables");
    HTable table = getHTable();
    Scan scan = getScan();
    if (table == null) {
      throw new IOException("No table was provided.");
    }
    
    if (!table.getTableDescriptor().isSalted()) {
      throw new IOException("table:" + Bytes.toString(table.getTableName())
          + " must be salted table");
    }
    
    return SaltedTableMapReduceUtil.getSplitsForSaltedTable(table, scan);
  }
    
  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context)
  throws IOException {
    return SaltedTableMapReduceUtil.createRecordReaderForSaltedTable(getHTable(),
      getTableRecordReader(), getScan(), split, context);
  }
}

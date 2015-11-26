package org.apache.hadoop.hbase.mapreduce.salted;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.xiaomi.infra.hbase.salted.SaltedHTable;
import com.xiaomi.infra.hbase.salted.SaltedHTable.SlotsWritable;

public class SaltedTableMapReduceUtil {
  private static final Log LOG = LogFactory.getLog(SaltedTableInputFormat.class);
  
  public static List<InputSplit> getSplitsForSaltedTable(HTable table, Scan scan)
      throws IOException {
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
      String regionLocation = table.getRegionLocation(slots[i]).getHostname();
      // splitStop is meaningless
      InputSplit split = new TableSplit(table.getFullTableName(), scan, slots[i], null, regionLocation);
      splits.add(split);
      if (LOG.isDebugEnabled()) {
        LOG.warn("getSplits for salted Table: split -> " + i + " -> " + split);
      }
    }
    return splits;
  }
  
  public static RecordReader<ImmutableBytesWritable, Result> createRecordReaderForSaltedTable(
      TableRecordReader reader, Scan scan, TableSplit split, TaskAttemptContext context)
      throws IOException {
    return createRecordReaderForSaltedTable(
      new HTable(context.getConfiguration(), split.getTableName()), reader, scan, split, context);
  }
  
  public static RecordReader<ImmutableBytesWritable, Result> createRecordReaderForSaltedTable(
      HTable table, TableRecordReader reader, Scan scan, InputSplit split,
      TaskAttemptContext context) throws IOException {
    if (table == null) {
      throw new IOException("Cannot create a record reader because of a"
          + " previous error. Please look at the previous logs lines from"
          + " the task's full log for more details.");
    }
    SaltedHTable saltedHTable = new SaltedHTable(table);

    TableSplit tSplit = (TableSplit) split;
    TableRecordReader trr = reader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new TableRecordReader();
    }
    Scan sc = new Scan(scan);
    SlotsWritable slotsWritable = new SlotsWritable(new byte[][] { tSplit.getStartRow() });
    sc.setAttribute(SaltedHTable.SLOTS_IN_SCAN, Writables.getBytes(slotsWritable));
    trr.setScan(sc);
    trr.setHTable(saltedHTable);
    try {
      trr.initialize(tSplit, context);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    }
    return trr;
  }
  
  public static boolean isSaltedTable(Configuration conf, byte[] tableName) throws IOException {
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      HTableDescriptor desc = table.getTableDescriptor();
      return desc.isSalted();
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }
}

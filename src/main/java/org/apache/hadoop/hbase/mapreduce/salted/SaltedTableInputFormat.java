package org.apache.hadoop.hbase.mapreduce.salted;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.xiaomi.infra.hbase.salted.SaltedHTable;
import com.xiaomi.infra.hbase.salted.SaltedHTable.SlotsWritable;


public class SaltedTableInputFormat extends TableInputFormat {
  private static final Log LOG = LogFactory.getLog(SaltedTableInputFormat.class);
  private TableRecordReader reader;
  private SaltedHTable saltedHTable;
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    HTable table = getHTable();
    Scan scan = getScan();
    if (table == null) {
      throw new IOException("No table was provided.");
    }
    
    if (!table.getTableDescriptor().isSalted()) {
      throw new IOException("table:" + Bytes.toString(table.getTableName())
          + " must be salted table");
    }
    
    // Get the name server address and the default value is null.
    this.nameServer = context.getConfiguration().get("hbase.nameserver.address", null);

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
      HServerAddress regionServerAddress = table.getRegionLocation(slots[i])
          .getServerAddress();
      InetAddress regionAddress = regionServerAddress.getInetSocketAddress().getAddress();
      String regionLocation;
      try {
        regionLocation = reverseDNS(regionAddress);
      } catch (NamingException e) {
        LOG.error("Cannot resolve the host name for " + regionAddress + " because of " + e);
        regionLocation = regionServerAddress.getHostname();
      }

      // splitStop is meaningless
      InputSplit split = new TableSplit(table.getTableName(), slots[i], null, regionLocation);
      splits.add(split);
      if (LOG.isDebugEnabled()) {
        LOG.debug("getSplits: split -> " + i + " -> " + split);
      }
    }
    return splits;
  }
    
  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
      InputSplit split, TaskAttemptContext context)
  throws IOException {
    // TODO : close htable and saltedhtable?
    if (getHTable() == null) {
      throw new IOException("Cannot create a record reader because of a" +
          " previous error. Please look at the previous logs lines from" +
          " the task's full log for more details.");
    }
    saltedHTable = new SaltedHTable(getHTable());
    
    TableSplit tSplit = (TableSplit) split;
    TableRecordReader trr = this.reader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new TableRecordReader();
    }
    Scan sc = new Scan(getScan());
    SlotsWritable slotsWritable = new SlotsWritable(new byte[][]{tSplit.getStartRow()});
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
}

/**
 *
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
package com.xiaomi.infra.hbase.salted;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Check;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;

/**
 * This operator is used to access(get, delete, put, scan) salted table easily.
 *
 */
public class SaltedHTable implements HTableInterface{
  public static final String SLOTS_IN_SCAN = "__salted_slots_in_scan__";
  private static Map<ImmutableBytesWritable, KeySalter> saltedTables =
      new ConcurrentHashMap<ImmutableBytesWritable, KeySalter>();
  
  private KeySalter salter;
  private HTableInterface table;

  public SaltedHTable(HTableInterface table) throws IOException {
    this(table, getKeySalter(HConnectionManager.getConnection(table.getConfiguration()),
      table.getTableName()));
  }
  
  public SaltedHTable(HTableInterface table, KeySalter salter) {
    this.table = table;
    this.salter = salter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(Get get) throws IOException {
    return unSalt(table.get(salt(get)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    byte[] slotsValue = scan.getAttribute(SLOTS_IN_SCAN);
    if (slotsValue == null) {
      return getScanner(scan, null);
    } else {
      SlotsWritable slotsWritable = new SlotsWritable();
      Writables.getWritable(slotsValue, slotsWritable);
      return getScanner(scan, slotsWritable.getSlots());
    }
  }

  /**
   * Allow to scan on specified salts.
   * @param scan
   * @param salts
   * @return
   * @throws IOException
   */
  public ResultScanner getScanner(Scan scan, byte[][] salts) throws IOException {
    return new SaltedScanner(scan, salts, false);
  }

 /**
   * Allow to scan on specified salts.
   * @param scan
   * @param salts
   * @param keepSalt, whether to keep the salt in the key.
   * @return
   * @throws IOException
   */
  public ResultScanner getScanner(Scan scan, byte[][] salts, boolean keepSalt) throws IOException {
    return new SaltedScanner(scan, salts, keepSalt);
  }

  public ResultScanner getScanner(Scan scan, byte[][] salts, boolean keepSalt, boolean merge)
      throws IOException {
    return new SaltedScanner(scan, salts, keepSalt, merge);
  }
  

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(Put put) throws IOException {
    table.put(salt(put));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(Delete delete) throws IOException {
    table.delete(salt(delete));
  }

  private Get salt(Get get) throws IOException {
    if (null == get) {
      return null;
    }
    Get newGet = new Get(salter.salt(get.getRow()));
    newGet.setFilter(get.getFilter());
    newGet.setCacheBlocks(get.getCacheBlocks());
    newGet.setMaxVersions(get.getMaxVersions());
    newGet.setTimeRange(get.getTimeRange().getMin(), get.getTimeRange().getMax());
    newGet.getFamilyMap().putAll(get.getFamilyMap());
    return newGet;
  }

  private Delete salt(Delete delete) {
    if (null == delete) {
      return null;
    }
    byte[] newRow = salter.salt(delete.getRow());
    Delete newDelete = new Delete(newRow);

    Map<byte[], List<KeyValue>> newMap = salt(delete.getFamilyMap());
    newDelete.getFamilyMap().putAll(newMap);
    return newDelete;
  }

  private Put salt(Put put) {
    if (null == put) {
      return null;
    }
    byte[] newRow = salter.salt(put.getRow());
    Put newPut = new Put(newRow); // put.ts: the put.ts won't be used if don't invoke add method
    Map<byte[], List<KeyValue>> newMap = salt(put.getFamilyMap());
    newPut.getFamilyMap().putAll(newMap);
    newPut.setDurability(put.getDurability());
    for (Map.Entry<String, byte[]> entry : put.getAttributesMap().entrySet()) {
      newPut.setAttribute(entry.getKey(), entry.getValue());
    }
    return newPut;
  }

  private Map<byte[], List<KeyValue>> salt(Map<byte[], List<KeyValue>> familyMap) {
    if (null == familyMap) {
      return null;
    }
    Map<byte[], List<KeyValue>> result = new HashMap<byte[], List<KeyValue>>();
    for (Map.Entry<byte[], List<KeyValue>> entry :
      familyMap.entrySet()) {
      List<KeyValue> kvs = entry.getValue();
      if (null != kvs) {
        List<KeyValue> newKvs = new ArrayList<KeyValue>();
        for (int i = 0; i < kvs.size(); i++) {
          newKvs.add(salt(kvs.get(i)));
        }
        result.put(entry.getKey(), newKvs);
      }
    }
    return result;
  }

  private KeyValue salt(KeyValue kv) {
    if (null == kv) {
      return null;
    }
    byte[] newRow = salter.salt(kv.getRow());
    return new KeyValue(newRow, 0,
        newRow.length,
        kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
        kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
        kv.getTimestamp(), KeyValue.Type.codeToType(kv.getType()),
        kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
  }

  private Result unSalt(Result result) {
    if (null == result) {
      return null;
    }
    KeyValue[] results = result.raw();
    if (null == results) {
      return null;
    }
    KeyValue[] newResults = new KeyValue[results.length];

    for (int i = 0; i < results.length; i++) {
      newResults[i] = unSalt(results[i]);
    }
    return new Result(newResults);
  }

  private KeyValue unSalt(KeyValue kv) {
    if (null == kv) {
      return null;
    }
    byte[] newRowKey = salter.unSalt(kv.getRow());
    return new KeyValue(newRowKey, 0, newRowKey.length,
        kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
        kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
        kv.getTimestamp(), KeyValue.Type.codeToType(kv.getType()),
        kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
  }

  public HTableInterface getRawTable() {
    return this.table;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getTableName() {
    return table.getTableName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Configuration getConfiguration() {
    return table.getConfiguration();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return table.getTableDescriptor();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoFlush() {
    return table.isAutoFlush();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushCommits() throws IOException {
    table.flushCommits();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    table.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists(Get get) throws IOException {
    Get newGet = salt(get);
    return table.exists(newGet);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    if (null == gets || gets.size() == 0) {
      return null;
    }
    Result[] result = new Result[gets.size()];
    for (int i = 0; i < gets.size(); i++) {
      Get newGet = salt(gets.get(i));
      result[i] = unSalt(table.get(newGet));
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(List<Put> puts) throws IOException {
    if (null == puts || puts.size() == 0) {
      return;
    }
    List<Put> newPuts = new ArrayList<Put>(puts.size());
    for (int i = 0; i < puts.size(); i++) {
      newPuts.add(salt(puts.get(i)));
    }
    table.put(newPuts);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(List<Delete> deletes) throws IOException {
    if (null == deletes ||deletes.size() == 0) {
      return;
    }
    List<Delete> newDeletes = new ArrayList<Delete>(deletes.size());
    for (int i = 0; i < deletes.size(); i++) {
      newDeletes.add(salt(deletes.get(i)));
    }
    table.delete(newDeletes);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result append(Append append) throws IOException {
    Result result = table.append(salt(append));
    return unSalt(result);
  }

  private Append salt(Append append) {
    if (null == append) {
      return null;
    }
    byte[] newRow = salter.salt(append.getRow());
    Append newAppend = new Append(newRow);

    Map<byte[], List<KeyValue>> newMap = salt(append.getFamilyMap());
    newAppend.getFamilyMap().putAll(newMap);
    return newAppend;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    byte[] newRow = salter.salt(row);
    Result result = table.getRowOrBefore(newRow, family);
    return unSalt(result);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    byte[] newRow = salter.salt(row);
    Put newPut = salt(put);
    return table.checkAndPut(newRow, family, qualifier, value, newPut);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareFilter.CompareOp compareOp, final byte [] value,
      final Put put) throws IOException {
    byte[] newRow = salter.salt(row);
    Put newPut = salt(put);
    return table.checkAndPut(newRow, family, qualifier, compareOp, value, newPut);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    byte[] newRow = salter.salt(row);
    Delete newDelete = salt(delete);
    return table.checkAndDelete(newRow, family, qualifier, value, newDelete);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareFilter.CompareOp compareOp, final byte [] value,
      final Delete delete) throws IOException {
    byte[] newRow = salter.salt(row);
    Delete newDelete = salt(delete);
    return table.checkAndDelete(newRow, family, qualifier, compareOp, value, newDelete);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException {
    byte[] newRow = salter.salt(row);
    return table.incrementColumnValue(newRow, family, qualifier, amount);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException {
    byte[] newRow = salter.salt(row);
    return table.incrementColumnValue(newRow, family, qualifier, amount, writeToWAL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result increment(Increment increment) throws IOException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * This scanner will merge sort the scan result, and remove the salts
   *
   */
  private class SaltedScanner implements ResultScanner {

    private BaseSaltedScanner scanner;
    private boolean keepSalt;

    public SaltedScanner (Scan scan, byte[][] salts, boolean keepSalt) throws IOException {
      this(scan, salts, keepSalt, true);
    }
    
    public SaltedScanner (Scan scan, byte[][] salts, boolean keepSalt, boolean merge) throws IOException {
      Scan[] scans = salt(scan, salts);
      this.keepSalt = keepSalt;
      if (merge) {
        this.scanner = new MergeSortScanner(scans, table, salter.getSaltLength());
      } else {
        this.scanner = new OrderSaltedScanner(scans, table);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Result> iterator() {
      return new Iterator<Result>() {

        public boolean hasNext() {
          return scanner.iterator().hasNext();
        }

        public Result next() {
          if (keepSalt) {
            return scanner.iterator().next();
          }
          else {
            return unSalt(scanner.iterator().next());
          }
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result next() throws IOException {
      if (keepSalt) {
        return scanner.next();
      }
      else {
        return unSalt(scanner.next());
      }
    }

     /**
      * {@inheritDoc}
      */
     @Override
    public Result[] next(int nbRows) throws IOException {
      ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
      for(int i = 0; i < nbRows; i++) {
        Result next = next();
        if (next != null) {
          resultSets.add(next);
        } else {
          break;
        }
      }
      return resultSets.toArray(new Result[resultSets.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
      scanner.close();
    }

    private Scan[] salt(Scan scan, byte[][] salts) throws IOException {

      byte[][] splits = null;
      if (null != salts) {
        splits = salts;
      }
      else {
        splits = salter.getAllSalts();
      }
      Scan[] scans = new Scan[splits.length];
      byte[] start = scan.getStartRow();
      byte[] end = scan.getStopRow();

      for (int i = 0; i < splits.length; i++) {
        scans[i] = new Scan(scan);
        scans[i].setStartRow(concat(splits[i], start));
        if (end.length == 0) {
          scans[i].setStopRow( (i == splits.length - 1) ?
              HConstants.EMPTY_END_ROW : splits[i + 1]);
        }
        else {
          scans[i].setStopRow(concat(splits[i], end));
        }
      }
      return scans;
    }

    private byte[] concat(byte[] prefix, byte[] row) {
      if (null == prefix || prefix.length == 0) {
        return row;
      }
      if (null == row || row.length == 0) {
        return prefix;
      }
      byte[] newRow = new byte[row.length + prefix.length];
      if (row.length != 0) {
        System.arraycopy(row, 0, newRow, prefix.length, row.length);
      }
      if (prefix.length != 0) {
        System.arraycopy(prefix, 0, newRow, 0, prefix.length);
      }
      return newRow;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    table.setAutoFlush(autoFlush, clearBufferOnFail);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    table.setWriteBufferSize(writeBufferSize);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getWriteBufferSize() {
    return table.getWriteBufferSize();
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush) {
    setAutoFlush(autoFlush, autoFlush);
  }

  @Override
  public byte[] getFullTableName() {
    return table.getFullTableName();
  }

  @Override
  public Result[] parallelGet(List<Get> gets) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean checkAndMutate(Check check, Mutation mutate) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public RowLock lockRow(byte[] row) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void unlockRow(RowLock rl) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol,
      byte[] startKey, byte[] endKey, Call<T, R> callable) throws IOException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> protocol,
      byte[] startKey, byte[] endKey, Call<T, R> callable, Callback<R> callback)
      throws IOException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }
  
  protected KeySalter getKeySalter() {
    return this.salter;
  }
  
  public static KeySalter createKeySalter(String keySalterClsName, Integer slotsCount)
      throws IOException {
    try {
      if (slotsCount == null) {
        return (KeySalter) Class.forName(keySalterClsName).newInstance();
      } else {
        return (KeySalter) Class.forName(keySalterClsName).getConstructor(int.class)
            .newInstance(slotsCount);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static class NotKeySalter implements KeySalter {
    @Override
    public int getSaltLength() {
      throw new RuntimeException("not implemented");
    }
    @Override
    public byte[][] getAllSalts() {
      throw new RuntimeException("not implemented");
    }
    @Override
    public byte[] salt(byte[] rowKey) {
      throw new RuntimeException("not implemented");
    }
    @Override
    public byte[] unSalt(byte[] row) {
      throw new RuntimeException("not implemented");
    }
  }
  
  // TODO : how to update the cache when recreated a table with salted attribute modified.
  //        Currently, we must restart the client to know the salted attribute change.
  public static KeySalter getKeySalter(HConnection connection, byte[] tableName) throws IOException {
    ImmutableBytesWritable tableNameAsKey = new ImmutableBytesWritable(tableName);
    if (saltedTables.containsKey(tableNameAsKey)) {
      KeySalter salter = saltedTables.get(tableNameAsKey);
      return salter instanceof NotKeySalter ? null : salter;
    } else {
      HTableDescriptor desc = connection.getHTableDescriptor(tableName);
      if (desc.isSalted()) {
        KeySalter salter = createKeySalter(desc.getKeySalter(), desc.getSlotsCount());
        saltedTables.put(tableNameAsKey, salter);
        return salter;
      } else {
        saltedTables.put(tableNameAsKey, new NotKeySalter());
        return null;
      }
    }
  }
  
  public static class SlotsWritable implements Writable {
    private byte[][] slots;
    
    public SlotsWritable() {}
    
    public SlotsWritable(byte[][] slots) {
      this.slots = slots;
    }
    
    public byte[][] getSlots() {
      return slots;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int slotCount = in.readInt();
      slots = new byte[slotCount][];
      for (int i = 0; i < slotCount; ++i) {
        slots[i] = Bytes.readByteArray(in);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(slots.length);
      for (byte[] slot : slots) {
        Bytes.writeByteArray(out, slot);
      }
    }
  }
}

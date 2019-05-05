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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.yetus.audience.InterfaceAudience;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * This operator is used to access(get, delete, put, scan) salted table easily.
 *
 */
@InterfaceAudience.Private
public class SaltedHTable implements Table {
  public static final String SLOTS_IN_SCAN = "__salted_slots_in_scan__";
  // KeySalter of table on cluster
  private static ConcurrentHashMap<String, Map<String, KeySalter>> saltedTables =
      new ConcurrentHashMap<>();

  private KeySalter salter;
  private Table table;

  public SaltedHTable(Table table) throws IOException {
    this(table, getKeySalter(table));
  }

  public SaltedHTable(Table table, KeySalter salter) {
    this.table = table;
    this.salter = salter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(Get get) throws IOException {
    checkNoRowFilter(get.getFilter());
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

    Map<byte[], List<Cell>> newMap = salt(delete.getFamilyCellMap());
    newDelete.getFamilyCellMap().putAll(newMap);
    return newDelete;
  }

  private Put salt(Put put) {
    if (null == put) {
      return null;
    }
    byte[] newRow = salter.salt(put.getRow());
    Put newPut = new Put(newRow); // put.ts: the put.ts won't be used if don't invoke add method
    Map<byte[], List<Cell>> newMap = salt(put.getFamilyCellMap());
    newPut.getFamilyCellMap().putAll(newMap);
    newPut.setDurability(put.getDurability());
    for (Map.Entry<String, byte[]> entry : put.getAttributesMap().entrySet()) {
      newPut.setAttribute(entry.getKey(), entry.getValue());
    }
    return newPut;
  }

  private Map<byte[], List<Cell>> salt(Map<byte[], List<Cell>> familyMap) {
    if (null == familyMap) {
      return null;
    }
    Map<byte[], List<Cell>> result = new HashMap<byte[], List<Cell>>();
    for (Map.Entry<byte[], List<Cell>> entry :
        familyMap.entrySet()) {
      List<Cell> cells = entry.getValue();
      if (null != cells) {
        List<Cell> newKvs = new ArrayList<Cell>();
        for (int i = 0; i < cells.size(); i++) {
          KeyValue kv = KeyValueUtil.ensureKeyValue(cells.get(i));
          newKvs.add(salt(kv));
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
    byte[] newRow = salter.salt(CellUtil.cloneRow(kv));
    return new KeyValue(newRow, 0,
        newRow.length,
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
        kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
        kv.getTimestamp(), KeyValue.Type.codeToType(kv.getTypeByte()),
        kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
  }

  private Result unSalt(Result result) {
    if (null == result) {
      return null;
    }
    Cell[] results = result.rawCells();
    if (null == results) {
      return null;
    }
    KeyValue[] newResults = new KeyValue[results.length];

    for (int i = 0; i < results.length; i++) {
      newResults[i] = unSalt(results[i]);
    }
    return Result.create(newResults);
  }

  private KeyValue unSalt(Cell kv) {
    if (null == kv) {
      return null;
    }
    byte[] newRowKey = salter.unSalt(CellUtil.cloneRow(kv));
    return new KeyValue(newRowKey, 0, newRowKey.length,
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(),
        kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
        kv.getTimestamp(), KeyValue.Type.codeToType(kv.getTypeByte()),
        kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
  }

  public Table getRawTable() {
    return this.table;
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

  @Override
  public TableDescriptor getDescriptor() throws IOException {
    return table.getDescriptor();
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
      checkNoRowFilter(gets.get(i).getFilter());
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

    Map<byte[], List<Cell>> newMap = salt(append.getFamilyCellMap());
    newAppend.getFamilyCellMap().putAll(newMap);
    return newAppend;
  }

  private Increment salt(Increment increment) {
    if (null == increment) {
      return null;
    }
    byte[] newRow = salter.salt(increment.getRow());
    Increment newIncrement = new Increment(newRow);
    Map<byte[], List<Cell>> newMap = salt(increment.getFamilyCellMap());
    newIncrement.getFamilyCellMap().putAll(newMap);
    return newIncrement;
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

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, Put put) throws IOException {
    byte[] newRow = salter.salt(row);
    Put newPut = salt(put);
    return table.checkAndPut(newRow, family, qualifier, compareOp, value, newPut);
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
      byte[] value, Put put) throws IOException {
    byte[] newRow = salter.salt(row);
    Put newPut = salt(put);
    return table.checkAndPut(newRow, family, qualifier, op, value, newPut);
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

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, Delete delete) throws IOException {
    byte[] newRow = salter.salt(row);
    Delete newDelete = salt(delete);
    return table.checkAndDelete(newRow, family, qualifier, compareOp, value, newDelete);
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
      byte[] value, Delete delete) throws IOException {
    byte[] newRow = salter.salt(row);
    Delete newDelete = salt(delete);
    return table.checkAndDelete(newRow, family, qualifier, op, value, newDelete);
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    throw new UnsupportedOperationException("not implemented");
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
  public Result increment(Increment increment) throws IOException {
    Result result = table.increment(salt(increment));
    return unSalt(result);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    List saltedActions = new ArrayList();
    for (int i = 0; i < actions.size(); ++i) {
      Object action = actions.get(i);
      if (action instanceof Put) {
        saltedActions.add(salt((Put)action));
      } else if (action instanceof Delete) {
        saltedActions.add(salt((Delete)action));
      } else if (action instanceof Get) {
        saltedActions.add(salt((Get)action));
      } else if (action instanceof Increment) {
        saltedActions.add(salt((Increment)action));
      } else if (action instanceof Append) {
        saltedActions.add(salt((Append)action));
      } else {
        throw new IOException("Row in batch must be Put/Get/Delete/Increment/Append for salted table");
      }
    }

    table.batch(saltedActions, results);
    for (int i = 0; i < results.length; ++i) {
      Object result = results[i];
      if (result != null && result instanceof Result) {
        results[i] = unSalt((Result)result);
      }
    }
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
      checkNoRowFilter(scan.getFilter());
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

    @Override
    public boolean renewLease() {
      throw new UnsupportedOperationException("not implemented");
    }

    // TODO: add flag to indicate the user passed salts is successive?
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
          // the salts passed by users might not be successive
          byte[] nextSalt = salter.nextSalt(splits[i]);
          scans[i].setStopRow(nextSalt == null ? HConstants.EMPTY_BYTE_ARRAY : nextSalt);
        } else {
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

    @Override
    public ScanMetrics getScanMetrics() {
      return null;
    }
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
    @Override
    public byte[] nextSalt(byte[] salt) {
      throw new RuntimeException("not implemented");
    }
    @Override
    public byte[] lastSalt(byte[] salt) {
      throw new RuntimeException("not implemented");
    }
    @Override
    public byte[] getSalt(byte[] rowKey) {
      throw new RuntimeException("not implemented");
    }
  }

  // TODO : how to update the cache when recreated a table with salted attribute modified.
  //        Currently, we must restart the client to know the salted attribute change.
  public static KeySalter getKeySalter(Table hTable) throws IOException {
    // tables with the same name in different clusters may have different slats attributes, we
    // use the full table name as key to cache table descriptor
    // znode parent and table name can identity a table among clusters
    String znodeParent = hTable.getConfiguration().get(HConstants.ZOOKEEPER_ZNODE_PARENT);
    Map<String, KeySalter> tableSalters = saltedTables.get(znodeParent);
    if (tableSalters == null) {
      tableSalters = new ConcurrentHashMap<String, KeySalter>();
      Map<String, KeySalter> previous = saltedTables.putIfAbsent(znodeParent, tableSalters);
      if (previous != null) {
        tableSalters = previous;
      }
    }
    String tableName = hTable.getName().getNameAsString();

    KeySalter salter = tableSalters.get(tableName);
    if (salter != null) {
      return salter instanceof NotKeySalter ? null : salter;
    } else {
      HTableDescriptor desc = hTable.getTableDescriptor();
      if (desc.isSalted()) {
        salter = createKeySalter(desc.getKeySalter(), desc.getSlotsCount());
        tableSalters.put(tableName, salter);
        return salter;
      } else {
        tableSalters.put(tableName, new NotKeySalter());
        return null;
      }
    }
  }

  @Override
  public TableName getName() {
    return table.getName();
  }

  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    if (null == gets || gets.size() == 0) {
      return null;
    }
    boolean[] result = new boolean[gets.size()];
    for (int i = 0; i < gets.size(); i++) {
      Get newGet = salt(gets.get(i));
      result[i] = table.exists(newGet);
    }
    return result;
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability) throws IOException {
    byte[] newRow = salter.salt(row);
    return table.incrementColumnValue(newRow, family, qualifier, amount, durability);
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service,
      byte[] startKey, byte[] endKey, Call<T, R> callable) throws ServiceException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable, Callback<R> callback) throws ServiceException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey,
      R responsePrototype) throws ServiceException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public <R extends Message> void batchCoprocessorService(MethodDescriptor methodDescriptor,
      Message request, byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp,
      byte[] value, RowMutations mutation) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
      byte[] value, RowMutations mutation) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long getRpcTimeout(TimeUnit unit) {
    return getRawTable().getRpcTimeout(unit);
  }

  @Override
  @Deprecated
  public int getRpcTimeout() {
    return getRawTable().getRpcTimeout();
  }

  @Override
  @Deprecated
  public void setRpcTimeout(int rpcTimeout) {
    setReadRpcTimeout(rpcTimeout);
    setWriteRpcTimeout(rpcTimeout);
  }

  @Override
  public long getReadRpcTimeout(TimeUnit unit) {
    return getRawTable().getReadRpcTimeout(unit);
  }

  @Override
  @Deprecated
  public int getReadRpcTimeout() {
    return getRawTable().getReadRpcTimeout();
  }

  @Override
  @Deprecated
  public void setReadRpcTimeout(int readRpcTimeout) {
    getRawTable().setReadRpcTimeout(readRpcTimeout);
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit unit) {
    return getRawTable().getWriteRpcTimeout(unit);
  }

  @Override
  @Deprecated
  public int getWriteRpcTimeout() {
    return getRawTable().getWriteRpcTimeout();
  }

  @Override
  @Deprecated
  public void setWriteRpcTimeout(int writeRpcTimeout) {
    getRawTable().setWriteRpcTimeout(writeRpcTimeout);
  }

  @Override
  public long getOperationTimeout(TimeUnit unit) {
    return getRawTable().getOperationTimeout(unit);
  }

  @Override
  @Deprecated
  public int getOperationTimeout() {
    return getRawTable().getOperationTimeout();
  }

  @Override
  @Deprecated
  public void setOperationTimeout(int operationTimeout) {
    getRawTable().setOperationTimeout(operationTimeout);
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

  // Salted table will add salt as prefix of user's rowkey, the salted/unsalted logic
  // is implemented purely in client-side. However, the Filter logic is implemented in
  // server side. So, if users set filters on row when reading from salted table, the
  // server won't get the right result. And it is not easy to add salt to filter in
  // client side. Now, we throw DNRIOE for such case, and will study smarter way when
  // needed(Currently, there are no users use row level Filter on salted table).
  protected void checkNoRowFilter(Filter filter) throws DoNotRetryIOException {
    if (filter == null) {
      return;
    }

    if (filter instanceof FilterList) {
      for (Filter filterInList : ((FilterList)filter).getFilters()) {
        checkNoRowFilter(filterInList);
      }
      return;
    }

    if (isRowFilter(filter)) {
      String tableName = table == null ? "" : Bytes.toString(table.getName().getName());
      throw new DoNotRetryIOException(
          "Filter on row is not allowed when reading salted table, table=" + tableName
              + ", filterClass=" + filter.getClass().getName());
    } else if (isWrapperFilter(filter)) {
      checkNoRowFilter(getWrappedFilter(filter));
    }
  }

  protected static boolean isRowFilter(Filter filter) {
    return (filter instanceof PrefixFilter) || (filter instanceof RowFilter)
        || (filter instanceof FuzzyRowFilter);
  }

  protected static boolean isWrapperFilter(Filter filter) {
    return (filter instanceof WhileMatchFilter) || (filter instanceof SkipFilter);
  }

  protected static Filter getWrappedFilter(Filter filter) {
    Filter wrappedFilter = null;
    if (filter instanceof WhileMatchFilter) {
      wrappedFilter = ((WhileMatchFilter)filter).getFilter();
    } else if (filter instanceof SkipFilter) {
      wrappedFilter = ((SkipFilter)filter).getFilter();
    }
    return wrappedFilter;
  }

  @Override
  public RegionLocator getRegionLocator() throws IOException {
    // The locator for SaltedHTable seems useless, so throw UNOE for now.
    throw new UnsupportedOperationException();
  }
}

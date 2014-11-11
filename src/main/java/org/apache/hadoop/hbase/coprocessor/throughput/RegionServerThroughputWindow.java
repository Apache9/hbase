/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.coprocessor.throughput;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.xiaomi.infra.hbase.throughput.RegionServerThroughputWindowMetrics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.throughput.ThroughputExceededException;
import org.apache.hadoop.hbase.util.Bytes;


 public class RegionServerThroughputWindow {

  public static enum RequestType {
    BEFORE_READ,
    AFTER_READ,
    WRITE
  }

  private long windowSize; //in milli seconds
  private final long maxReadNum;
  private final long maxWriteNum;

  private ConcurrentHashMap<String, AtomicLong> tableReadMap = new ConcurrentHashMap<String, AtomicLong>();
  private ConcurrentHashMap<String, AtomicLong> tableWriteMap = new ConcurrentHashMap<String, AtomicLong>();


  private AtomicLong numRead = new AtomicLong(0);
  private AtomicLong numWrite = new AtomicLong(0);

  private final Object lock = new Object();
  private volatile long startTime = System.currentTimeMillis();

  private static final byte[] ACL_TABLE_BYTES = Bytes.toBytes("_acl_");

  private static final Log LOG = LogFactory.getLog(RegionServerThroughputWindow.class);

  RegionServerThroughputWindowMetrics metrics = new RegionServerThroughputWindowMetrics();


  public RegionServerThroughputWindow(long windowSize, long maxReadQps, long maxWriteQps) {
    this.windowSize = windowSize;
    this.maxReadNum = maxReadQps * windowSize / 1000;
    this.maxWriteNum = maxWriteQps * windowSize / 1000;
  }

  private boolean isSystemTable(byte[] tableName) {
    return Bytes.compareTo(tableName, HConstants.ROOT_TABLE_NAME) == 0
        || Bytes.compareTo(tableName, HConstants.META_TABLE_NAME) == 0
        || Bytes.compareTo(tableName, ACL_TABLE_BYTES) == 0;
  }

  //Only be called for unit test
  public void reset() {
    synchronized (lock) {
      endWindow();
    }
  }

  public void check(byte[] tableName, RequestType requestType, int n) throws ThroughputExceededException {
    if(isSystemTable(tableName))
    {
      return;
    }

    if(System.currentTimeMillis() - startTime >= windowSize)
    {
      synchronized (lock)
      {
        if(System.currentTimeMillis() - startTime >= windowSize)
        {
          endWindow();
        }
      }
    }

    if(requestType.equals(RequestType.BEFORE_READ)) {
      checkBeforeRead(tableName, n);
    }
    else if(requestType.equals(RequestType.AFTER_READ)) {
      checkAfterRead(tableName, n);
    }
    else {
      checkBeforeWrite(tableName, n);
    }
  }

   public void check(byte[] tableName, RequestType requestType) throws ThroughputExceededException {
     check(tableName, requestType, 1);
   }

   public static String generateTableStatStr(ConcurrentHashMap<String, AtomicLong> statMap) {
     ArrayList<Map.Entry<String, AtomicLong>> ret =  new ArrayList<Map.Entry<String, AtomicLong>>();
     ret.addAll(statMap.entrySet());
     Collections.sort(ret, new Comparator<Map.Entry<String, AtomicLong>>() {
      @Override public int compare(Map.Entry<String, AtomicLong> o1, Map.Entry<String, AtomicLong> o2) {
        if(o1.getValue().get() == o2.getValue().get())
          return 0;
        else  if(o1.getValue().get() > o2.getValue().get())
          return -1;
        else
          return 1;
      }
    });

    StringBuffer sb = new StringBuffer();
    for(Map.Entry<String, AtomicLong> entry : ret)
    {
      sb.append(entry.getKey());
      sb.append(":");
      sb.append(entry.getValue().get());
      sb.append(";");
    }
    return sb.toString();
  }

  private void endWindow(){
    //print the table names order by read qps if reads exceed the quota

    if(LOG.isDebugEnabled()) {
      LOG.debug("reset RegionServerThroughputWindow, numRead=" + numRead + " numWrite=" + numWrite);
    }

    long currentRead = numRead.get();
    if (currentRead >= maxReadNum) {
      String tableStatMsg = generateTableStatStr(tableReadMap);
      if(LOG.isInfoEnabled()) {
        LOG.info("currentRead:" +currentRead + " maxReadNum:" + maxReadNum + " Read exceed:" + tableStatMsg);
      }
    }

    //print the table names order by write qps if writes exceed the quota
    long currentWrite = numWrite.get();
    if (currentWrite >= maxWriteNum) {
      String tableStatMsg = generateTableStatStr(tableWriteMap);
      if(LOG.isInfoEnabled()) {
        LOG.info("currentWrite:" + currentWrite + " maxWriteNum:" + maxWriteNum + " Write exceed:" + tableStatMsg);
      }

    }

    startTime = System.currentTimeMillis();
    metrics.readRecordsCount.set(numRead.get());
    metrics.writeRecordsCount.set(numWrite.get());

    numRead.set(0);
    numWrite.set(0);
    tableReadMap.clear();
    tableWriteMap.clear();
  }

  private void checkBeforeRead(byte[] tableName, int n) throws ThroughputExceededException {
    long currentRead = numRead.get();
    if (currentRead >= maxReadNum) {
      throw new ThroughputExceededException(
          "The request will be rejected due to exceeding maxReadQps, currentRead=" + currentRead
              + " maxReadNum=" + maxReadNum + " timestamp=" + System.currentTimeMillis()
      );
    }
  }

  private void checkAfterRead(byte[] tableName, int n) throws  ThroughputExceededException {
    long currentRead = numRead.addAndGet(n);
    AtomicLong tableReadNum = tableReadMap.get(Bytes.toString(tableName));
    if (tableReadNum == null) {
      tableReadNum = new AtomicLong(n);
      tableReadNum = tableReadMap.putIfAbsent(Bytes.toString(tableName), tableReadNum);
      if (tableReadNum != null) {
        tableReadNum.addAndGet(n);
      }
    } else {
      tableReadNum.addAndGet(n);
    }
  }

  private void checkBeforeWrite(byte[] tableName, int n) throws ThroughputExceededException {
    if(n == 0) {
      return;
    }
    long currentWrite = numWrite.getAndAdd(n);
    if (currentWrite >= maxWriteNum) {
      numWrite.addAndGet(-n);
      throw new ThroughputExceededException("The request will be rejected due to exceeding maxWriteQps, currentWrite=" + currentWrite
          + " maxWriteNum="+maxWriteNum +" timestamp=" + System.currentTimeMillis());
    }
    AtomicLong tableWriteNum = tableWriteMap.get(Bytes.toString(tableName));
    if (tableWriteNum == null) {
      tableWriteNum = new AtomicLong(n);
      tableWriteNum = tableWriteMap.putIfAbsent(Bytes.toString(tableName), tableWriteNum);
      if (tableWriteNum != null) {
        tableWriteNum.addAndGet(n);
      }
    } else {
      tableWriteNum.addAndGet(n);
    }
  }

}

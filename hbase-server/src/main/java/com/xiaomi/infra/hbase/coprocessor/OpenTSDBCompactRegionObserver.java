/*
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
package com.xiaomi.infra.hbase.coprocessor;

import com.xiaomi.infra.hbase.coprocessor.opentsdb.OpenTSDBCompaction;
import com.xiaomi.infra.hbase.coprocessor.opentsdb.OpenTSDBUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.xiaomi.infra.hbase.coprocessor.opentsdb.OpenTSDBUtil.MAX_TIMESPAN;

/**
 * Used for OpenTSDB data table compaction,
 * compact multi column/Cells into one column/Cell of one row.
 */
public class OpenTSDBCompactRegionObserver extends BaseRegionObserver {
  private static final Log LOG = LogFactory.getLog(OpenTSDBCompactRegionObserver.class);
  private final static long ONE_HOUR_SECONDS = MAX_TIMESPAN;
  private final static String TSDB_TABLE_CF = "t";

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final InternalScanner scanner, final ScanType scanType) throws IOException {
    if (!TSDB_TABLE_CF.equals(store.getColumnFamilyName())) {
      return scanner;
    }
    Configuration conf = e.getEnvironment().getConfiguration();
    // before hourWindow hours from now, need compact
    long hourWindow = conf.getInt("hbase.opentsdb.compaction.hour.before", 1);
    return new OpenTSDBCompactScanner(scanner, store, hourWindow);
  }

  class OpenTSDBCompactScanner implements InternalScanner {
    // is StoreScanner
    private InternalScanner scanner;
    // in seconds
    private final long compactTimeStamp;

    OpenTSDBCompactScanner(InternalScanner scanner, Store store, long hour) {
      this.scanner = scanner;
      // seconds. Compact before compactTimeStamp Cells
      this.compactTimeStamp = EnvironmentEdgeManager.currentTimeMillis() / 1000 - hour * ONE_HOUR_SECONDS - 1;
      LOG.info("Created OpenTSDBCompactScanner, table name=" + store.getTableName()
          + ", store cf=" + store.getColumnFamilyName()
          + ", compactTimeStamp=" + compactTimeStamp);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
      boolean hasMore = scanner.next(results);
      tryCompact(results);
      return hasMore;
    }

    @Override
    public boolean next(List<Cell> results, ScannerContext scannerContext) throws IOException {
      boolean hasMore = scanner.next(results, scannerContext);
      tryCompact(results);
      return hasMore;
    }

    @Override
    public void close() throws IOException {
      scanner.close();
    }

    /**
     * call opentsdb compaction method
     * @param results that keep the compacted cell or origin cells.
     * @return Compacted Cell
     */
    private void tryCompact(List<Cell> results) {
      if (results.isEmpty()) {
        return;
      }
      Cell first = results.get(0);
      byte[] rowkey = CellUtil.cloneRow(first);
      long baseTs = OpenTSDBUtil.getRowKeyBaseTime(first);
      if ((results.size() <= 1) || (baseTs > compactTimeStamp)) {
        // if row too recent, do not compact
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip compact row. Rowkey=" + Bytes.toStringBinary(rowkey)
              + ", key baseTs=" + baseTs + ", compactTimeStamp=" + compactTimeStamp
              + ", results size=" + results.size());
        }
        return;
      }
      // do compact one rowkey rowCells
      // Not datapoint cells do not compact, just return them unchanged.
      List<Cell> skipCompactCells = new ArrayList<Cell>();
      Cell compacted = new OpenTSDBCompaction(results, skipCompactCells).compact();
      if (null == compacted) {
        // something wrong when compact, just return
        if (LOG.isDebugEnabled()) {
          LOG.debug("Error compact. Rowkey=" + Bytes.toStringBinary(rowkey)
              + ", key baseTs=" + baseTs + ", compactTimeStamp=" + compactTimeStamp
              + ", results size=" + results.size());
        }
        return;
      }
      // delete all compacted cells
      results.clear();
      if (!skipCompactCells.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not datapoint cells found. "
              + "Rowkey=" + Bytes.toStringBinary(rowkey)
              + ", skipCompactCells size=" + skipCompactCells.size());
        }
        results.addAll(skipCompactCells);
      }
      results.add(compacted);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Success compact. Compacted Rowkey=" + Bytes.toStringBinary(rowkey)
            + ", key baseTs=" + baseTs + ", qualifierLen=" + compacted.getQualifierLength()
            + ", valueLen=" + compacted.getValueLength()
            + ", results size=" + results.size());
      }
    }
  }
}

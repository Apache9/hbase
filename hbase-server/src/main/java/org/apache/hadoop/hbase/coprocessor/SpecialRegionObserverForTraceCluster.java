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
package org.apache.hadoop.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *  Used for c3miuisrv-trace.
 *  Old timestamp: str(timestamp) + HOST_NAME_HASH + String.format("%03d", RANDOM.nextInt(1000))
 *  New timestamp: str(timestamp/10000000) + str(ts/300000) + stageId
 *
 *  So only use substr(timestamp, 0, 6) + 0000000 as the timestamp.
 */
public class SpecialRegionObserverForTraceCluster extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(StoreScanner.class);

  private final static long ONE_MILLION = 1000000L;
  private final static long TEN_MILLION = 10000000L;

  @Override
  public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final InternalScanner scanner, final ScanType scanType)
      throws IOException {
    return new SpecialScanner(scanner, store.getStoreFileTtl());
  }

  class SpecialScanner implements InternalScanner {

    private InternalScanner scanner;

    private final long oldestUnexpiredTS;

    SpecialScanner(InternalScanner scanner, long ttl) {
      this.scanner = scanner;
      // For safe, minus 2 * ttl
      this.oldestUnexpiredTS = EnvironmentEdgeManager.currentTimeMillis() - 2 * ttl;
      LOG.info("Created new scanner, ttl=" + ttl + ", oldestUnexpiredTS=" + oldestUnexpiredTS);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
      boolean hasMore = scanner.next(results);
      filterExpiredCells(results);
      return hasMore;
    }

    @Override
    public boolean next(List<Cell> results, ScannerContext scannerContext) throws IOException {
      boolean hasMore = scanner.next(results, scannerContext);
      filterExpiredCells(results);
      return hasMore;
    }

    @Override
    public void close() throws IOException {
      scanner.close();
    }

    private void filterExpiredCells(List<Cell> results) {
      Iterator<Cell> iterator = results.iterator();
      while (iterator.hasNext()) {
        Cell cell = iterator.next();
        LOG.debug("Cell timestamp is " + cell.getTimestamp());
        long rightTimestamp = toRightTimestamp(cell.getTimestamp());
        if (rightTimestamp < oldestUnexpiredTS) {
          iterator.remove();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Removed expired cell " + cell);
          }
        }
      }
    }

    /**
     * Use substr(timestamp, 0, 6) + 0000000 as the timestamp.
     */
    private long toRightTimestamp(long timestamp) {
      while (timestamp >= ONE_MILLION) {
        timestamp /= 10;
      }
      return timestamp * TEN_MILLION;
    }
  }
}

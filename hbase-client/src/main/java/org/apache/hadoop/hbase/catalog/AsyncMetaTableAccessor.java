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
package org.apache.hadoop.hbase.catalog;

import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader.CollectingVisitor;
import org.apache.hadoop.hbase.catalog.MetaReader.Visitor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RawAsyncTable;
import org.apache.hadoop.hbase.client.RawScanResultConsumer;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * The asynchronous meta table accessor. Used to read/write region and assignment information store
 * in <code>hbase:meta</code>.
 */
@InterfaceAudience.Private
public class AsyncMetaTableAccessor {

  private static final Log LOG = LogFactory.getLog(AsyncMetaTableAccessor.class);

  public static CompletableFuture<Boolean> tableExists(RawAsyncTable metaTable, TableName tableName) {
    if (tableName.equals(META_TABLE_NAME)) {
      return CompletableFuture.completedFuture(true);
    }
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    // Make a version of ResultCollectingVisitor that only collects the first
    CollectingVisitor<HRegionInfo> visitor = new CollectingVisitor<HRegionInfo>() {
      private HRegionInfo current = null;

      @Override
      public boolean visit(Result r) throws IOException {
        this.current =
          HRegionInfo.getHRegionInfo(r, HConstants.REGIONINFO_QUALIFIER);
        if (this.current == null) {
          LOG.warn("No serialized HRegionInfo in " + r);
          return true;
        }
        if (!MetaReader.isInsideTable(this.current, tableName)) return false;
        // Else call super and add this Result to the collection.
        super.visit(r);
        // Stop collecting regions from table after we get one.
        return false;
      }

      @Override
      void add(Result r) {
        // Add the current HRI.
        this.results.add(this.current);
      }
    };
    scanMeta(metaTable, Optional.of(tableName), visitor).whenComplete((v, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      future.complete(visitor.getResults().size() >= 1);
    });
    return future;
  }

  /**
   * Returns the HRegionLocation from meta for the given region
   * @param metaTable
   * @param regionName region we're looking for
   * @return HRegionLocation for the given region
   */
  public static CompletableFuture<Optional<HRegionLocation>> getRegionLocation(
      RawAsyncTable metaTable, byte[] regionName) {
    CompletableFuture<Optional<HRegionLocation>> future = new CompletableFuture<>();
    metaTable.get(new Get(regionName).addFamily(HConstants.CATALOG_FAMILY)).whenComplete(
      (r, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        future.complete(getRegionLocation(r));
      });
    return future;
  }

  private static Optional<HRegionLocation> getRegionLocation(Result result) {
    HRegionInfo hri = HRegionInfo.getHRegionInfo(result, HConstants.REGIONINFO_QUALIFIER);
    ServerName sn = HRegionInfo.getServerName(result);
    if (hri != null && sn != null) {
      return Optional.of(new HRegionLocation(hri, sn, HRegionInfo.getSeqNumDuringOpen(result)));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Returns the HRegionLocation from meta for the given encoded region name
   * @param metaTable
   * @param encodedRegionName region we're looking for
   * @return HRegionLocation for the given region
   */
  public static CompletableFuture<Optional<HRegionLocation>> getRegionLocationWithEncodedName(
      RawAsyncTable metaTable, byte[] encodedRegionName) {
    CompletableFuture<Optional<HRegionLocation>> future = new CompletableFuture<>();
    metaTable.scanAll(new Scan().setReadType(ReadType.PREAD).addFamily(HConstants.CATALOG_FAMILY))
        .whenComplete(
          (results, err) -> {
            if (err != null) {
              future.completeExceptionally(err);
              return;
            }
            String encodedRegionNameStr = Bytes.toString(encodedRegionName);
            results
                .stream()
                .filter(result -> !result.isEmpty())
                .forEach(
                  result -> {
                    getRegionLocation(result).ifPresent(
                      location -> {
                        if (location != null
                            && encodedRegionNameStr.equals(location.getRegionInfo()
                                .getEncodedName())) {
                          future.complete(Optional.of(location));
                          return;
                        }
                      });
                  });
            future.complete(Optional.empty());
          });
    return future;
  }

  /**
   * Used to get all region locations for the specific table.
   * @param metaTable
   * @param tableName table we're looking for, can be null for getting all regions
   * @return the list of region locations. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  public static CompletableFuture<List<HRegionLocation>> getTableHRegionLocations(
      RawAsyncTable metaTable, final Optional<TableName> tableName) {
    CompletableFuture<List<HRegionLocation>> future = new CompletableFuture<>();
    getTableRegionsAndLocations(metaTable, tableName, true).whenComplete(
      (locations, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else if (locations == null || locations.isEmpty()) {
          future.complete(Collections.emptyList());
        } else {
          List<HRegionLocation> regionLocations = locations.stream()
              .map(loc -> new HRegionLocation(loc.getFirst(), loc.getSecond()))
              .collect(Collectors.toList());
          future.complete(regionLocations);
        }
      });
    return future;
  }

  /**
   * Used to get table regions' info and server.
   * @param metaTable
   * @param tableName table we're looking for, can be null for getting all regions
   * @param excludeOfflinedSplitParents don't return split parents
   * @return the list of regioninfos and server. The return value will be wrapped by a
   *         {@link CompletableFuture}.
   */
  private static CompletableFuture<List<Pair<HRegionInfo, ServerName>>> getTableRegionsAndLocations(
      RawAsyncTable metaTable, final Optional<TableName> tableName,
      final boolean excludeOfflinedSplitParents) {
    CompletableFuture<List<Pair<HRegionInfo, ServerName>>> future = new CompletableFuture<>();
    if (tableName.filter((t) -> t.equals(TableName.META_TABLE_NAME)).isPresent()) {
      future.completeExceptionally(new IOException(
          "This method can't be used to locate meta regions;" + " use MetaTableLocator instead"));
    }

    // Make a version of CollectingVisitor that collects HRegionInfo and ServerAddress
    CollectingVisitor<Pair<HRegionInfo, ServerName>> visitor = new CollectingVisitor<Pair<HRegionInfo, ServerName>>() {
      private Optional<HRegionLocation> current = null;

      @Override
      public boolean visit(Result r) throws IOException {
        current = getRegionLocation(r);
        if (!current.isPresent()) {
          LOG.warn("No serialized HRegionInfo in " + r);
          return true;
        }
        HRegionInfo hri = current.get().getRegionInfo();
        if (tableName.isPresent() && !MetaReader.isInsideTable(hri, tableName.get())) {
          return false;
        }
        if (excludeOfflinedSplitParents && hri.isSplitParent()) return true;
        // Else call super and add this Result to the collection.
        return super.visit(r);
      }

      @Override
      void add(Result r) {
        if (!current.isPresent()) {
          return;
        }
        this.results.add(new Pair<HRegionInfo, ServerName>(current.get().getRegionInfo(), current
            .get().getServerName()));
      }
    };

    scanMeta(metaTable, tableName, visitor).whenComplete((v, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      future.complete(visitor.getResults());
    });
    return future;
  }

  /**
   * Performs a scan of META table for given table.
   * @param metaTable
   * @param tableName table withing we scan
   * @param type scanned part of meta
   * @param visitor Visitor invoked against each row
   */
  private static CompletableFuture<Void> scanMeta(RawAsyncTable metaTable,
      Optional<TableName> tableName, final Visitor visitor) {
    return scanMeta(metaTable, getTableStartRowForMeta(tableName),
      getTableStopRowForMeta(tableName), Integer.MAX_VALUE, visitor);
  }

  /**
   * Performs a scan of META table for given table.
   * @param metaTable
   * @param startRow Where to start the scan
   * @param stopRow Where to stop the scan
   * @param type scanned part of meta
   * @param maxRows maximum rows to return
   * @param visitor Visitor invoked against each row
   */
  private static CompletableFuture<Void> scanMeta(RawAsyncTable metaTable,
      Optional<byte[]> startRow, Optional<byte[]> stopRow, int maxRows, final Visitor visitor) {
    int rowUpperLimit = maxRows > 0 ? maxRows : Integer.MAX_VALUE;
    Scan scan = getMetaScan(metaTable, rowUpperLimit).addFamily(HConstants.CATALOG_FAMILY);
    startRow.ifPresent(scan::withStartRow);
    stopRow.ifPresent(scan::withStopRow);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Scanning META" + " starting at row=" + Bytes.toStringBinary(scan.getStartRow())
          + " stopping at row=" + Bytes.toStringBinary(scan.getStopRow()) + " for max="
          + rowUpperLimit + " with caching=" + scan.getCaching());
    }

    CompletableFuture<Void> future = new CompletableFuture<Void>();
    metaTable.scan(scan, new MetaTableRawScanResultConsumer(rowUpperLimit, visitor, future));
    return future;
  }

  private static final class MetaTableRawScanResultConsumer implements RawScanResultConsumer {

    private int currentRowCount;

    private final int rowUpperLimit;

    private final Visitor visitor;

    private final CompletableFuture<Void> future;

    MetaTableRawScanResultConsumer(int rowUpperLimit, Visitor visitor, CompletableFuture<Void> future) {
      this.rowUpperLimit = rowUpperLimit;
      this.visitor = visitor;
      this.future = future;
      this.currentRowCount = 0;
    }

    @Override
    public void onError(Throwable error) {
      future.completeExceptionally(error);
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NONNULL_PARAM_VIOLATION",
      justification = "https://github.com/findbugsproject/findbugs/issues/79")
    public void onComplete() {
      future.complete(null);
    }

    @Override
    public void onNext(Result[] results, ScanController controller) {
      for (Result result : results) {
        try {
          if (!visitor.visit(result)) {
            controller.terminate();
          }
        } catch (IOException e) {
          future.completeExceptionally(e);
          controller.terminate();
        }
        if (++currentRowCount >= rowUpperLimit) {
          controller.terminate();
        }
      }
    }
  }

  private static Scan getMetaScan(RawAsyncTable metaTable, int rowUpperLimit) {
    Scan scan = new Scan();
    int scannerCaching = metaTable.getConfiguration().getInt(HConstants.HBASE_META_SCANNER_CACHING,
      HConstants.DEFAULT_HBASE_META_SCANNER_CACHING);
    if (rowUpperLimit <= scannerCaching) {
      scan.setLimit(rowUpperLimit);
    }
    int rows = Math.min(rowUpperLimit, scannerCaching);
    scan.setCaching(rows);
    return scan;
  }

  /**
   * @param tableName table we're working with
   * @return start row for scanning META
   */
  public static Optional<byte[]> getTableStartRowForMeta(Optional<TableName> tableName) {
    return tableName.map((table) -> {
      byte[] startRow = new byte[table.getName().length + 2];
      System.arraycopy(table.getName(), 0, startRow, 0, table.getName().length);
      startRow[startRow.length - 2] = HConstants.DELIMITER;
      startRow[startRow.length - 1] = HConstants.DELIMITER;
      return startRow;
    });
  }

  /**
   * @param tableName table we're working with
   * @return stop row for scanning META
   */
  public static Optional<byte[]> getTableStopRowForMeta(Optional<TableName> tableName) {
    return tableName.map((table) -> {
      final byte[] stopRow = new byte[table.getName().length + 3];
      System.arraycopy(table.getName(), 0, stopRow, 0, table.getName().length);
      stopRow[stopRow.length - 3] = ' ';
      stopRow[stopRow.length - 2] = HConstants.DELIMITER;
      stopRow[stopRow.length - 1] = HConstants.DELIMITER;
      return stopRow;
    });
  }
}

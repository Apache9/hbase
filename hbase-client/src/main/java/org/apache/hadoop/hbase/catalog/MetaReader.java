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

import edu.umd.cs.findbugs.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Reads region and assignment information from <code>hbase:meta</code>.
 */
@InterfaceAudience.Private
public class MetaReader {
  // TODO: Strip CatalogTracker from this class.  Its all over and in the end
  // its only used to get its Configuration so we can get associated
  // Connection.
  static final Log LOG = LogFactory.getLog(MetaReader.class);

  static final byte [] META_REGION_PREFIX;
  static {
    // Copy the prefix from FIRST_META_REGIONINFO into META_REGION_PREFIX.
    // FIRST_META_REGIONINFO == 'hbase:meta,,1'.  META_REGION_PREFIX == 'hbase:meta,'
    int len = HRegionInfo.FIRST_META_REGIONINFO.getRegionName().length - 2;
    META_REGION_PREFIX = new byte [len];
    System.arraycopy(HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), 0,
      META_REGION_PREFIX, 0, len);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>, skipping regions from any
   * tables in the specified set of disabled tables.
   * @param conn
   * @param disabledTables set of disabled tables that will not be returned
   * @return Returns a map of every region to it's currently assigned server,
   * according to META.  If the region does not have an assignment it will have
   * a null value in the map.
   * @throws IOException
   */
  public static Map<HRegionInfo, ServerName> fullScan(HConnection conn,
      Set<TableName> disabledTables) throws IOException {
    return fullScan(conn, disabledTables, false);
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>, skipping regions from any
   * tables in the specified set of disabled tables.
   * @param conn
   * @param disabledTables set of disabled tables that will not be returned
   * @param excludeOfflinedSplitParents If true, do not include offlined split
   * parents in the return.
   * @return Returns a map of every region to it's currently assigned server,
   * according to META.  If the region does not have an assignment it will have
   * a null value in the map.
   * @throws IOException
   */
  public static Map<HRegionInfo, ServerName> fullScan(HConnection conn,
      Set<TableName> disabledTables, boolean excludeOfflinedSplitParents) throws IOException {
    final Map<HRegionInfo, ServerName> regions = new TreeMap<HRegionInfo, ServerName>();
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) return true;
        Pair<HRegionInfo, ServerName> region = HRegionInfo.getHRegionInfoAndServerName(r);
        HRegionInfo hri = region.getFirst();
        if (hri == null) return true;
        if (hri.getTable() == null) return true;
        if (disabledTables.contains(hri.getTable())) return true;
        // Are we to include split parents in the list?
        if (excludeOfflinedSplitParents && hri.isSplitParent()) return true;
        regions.put(hri, region.getSecond());
        return true;
      }
    };
    fullScan(conn, v);
    return regions;
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>.
   * @return List of {@link Result}
   * @throws IOException
   */
  public static List<Result> fullScan(HConnection conn) throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    fullScan(conn, v, null);
    return v.getResults();
  }

  /**
   * Performs a full scan of a <code>hbase:meta</code> table.
   * @return List of {@link Result}
   * @throws IOException
   */
  public static List<Result> fullScanOfMeta(HConnection conn) throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    fullScan(conn, v, null);
    return v.getResults();
  }

  /**
   * Performs a full scan of <code>hbase:meta</code>.
   * @param conn
   * @param visitor Visitor invoked against each row.
   * @throws IOException
   */
  public static void fullScan(HConnection conn, final Visitor visitor) throws IOException {
    fullScan(conn, visitor, null);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param conn
   * @param tableName Table to get an {@link HTable} against.
   * @return An {@link HTable} for <code>tableName</code>
   * @throws IOException
   */
  private static HTable getHTable(HConnection conn, TableName tableName) throws IOException {
    // Passing the CatalogTracker's connection ensures this
    // HTable instance uses the CatalogTracker's connection.
    if (conn == null) {
      throw new NullPointerException("No connection");
    }
    return new HTable(tableName, conn);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param conn
   * @return An {@link HTable} for <code>hbase:meta</code>
   * @throws IOException
   */
  public static HTable getCatalogHTable(HConnection conn) throws IOException {
    return getMetaHTable(conn);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param ct
   * @return An {@link HTable} for <code>hbase:meta</code>
   * @throws IOException
   */
  public static HTable getMetaHTable(HConnection conn) throws IOException {
    return getHTable(conn, TableName.META_TABLE_NAME);
  }

  /**
   * @param t Table to use (will be closed when done).
   * @param g Get to run
   * @throws IOException
   */
  private static Result get(final HTable t, final Get g) throws IOException {
    try {
      return t.get(g);
    } finally {
      t.close();
    }
  }

  /**
   * Reads the location of the specified region
   * @param conn
   * @param regionName region whose location we are after
   * @return location of region as a {@link ServerName} or null if not found
   * @throws IOException
   */
  static ServerName readRegionLocation(HConnection conn, byte[] regionName) throws IOException {
    Pair<HRegionInfo, ServerName> pair = getRegion(conn, regionName);
    return (pair == null || pair.getSecond() == null) ? null : pair.getSecond();
  }

  /**
   * Gets the region info and assignment for the specified region.
   * @param conn
   * @param regionName Region to lookup.
   * @return Location and HRegionInfo for <code>regionName</code>
   * @throws IOException
   */
  public static Pair<HRegionInfo, ServerName> getRegion(HConnection conn, byte[] regionName)
      throws IOException {
    Get get = new Get(regionName);
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result r = get(getCatalogHTable(conn), get);
    return (r == null || r.isEmpty()) ? null : HRegionInfo.getHRegionInfoAndServerName(r);
  }

  /**
   * Gets the result in hbase:meta for the specified region.
   * @param conn
   * @param regionName
   * @return result of the specified region
   * @throws IOException
   */
  public static Result getRegionResult(HConnection conn, byte[] regionName) throws IOException {
    Get get = new Get(regionName);
    get.addFamily(HConstants.CATALOG_FAMILY);
    return get(getCatalogHTable(conn), get);
  }

  /**
   * Get regions from the merge qualifier of the specified merged region
   * @return null if it doesn't contain merge qualifier, else two merge regions
   * @throws IOException
   */
  public static Pair<HRegionInfo, HRegionInfo> getRegionsFromMergeQualifier(HConnection conn,
      byte[] regionName) throws IOException {
    Result result = getRegionResult(conn, regionName);
    HRegionInfo mergeA = HRegionInfo.getHRegionInfo(result, HConstants.MERGEA_QUALIFIER);
    HRegionInfo mergeB = HRegionInfo.getHRegionInfo(result, HConstants.MERGEB_QUALIFIER);
    if (mergeA == null && mergeB == null) {
      return null;
    }
    return new Pair<HRegionInfo, HRegionInfo>(mergeA, mergeB);
  }

  /**
   * Checks if the specified table exists.  Looks at the hbase:meta table hosted on
   * the specified server.
   * @param conn
   * @param tableName table to check
   * @return true if the table exists in meta, false if not
   * @throws IOException
   */
  public static boolean tableExists(HConnection conn, TableName tableName) throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      // Catalog tables always exist.
      return true;
    }
    // Make a version of ResultCollectingVisitor that only collects the first
    CollectingVisitor<HRegionInfo> visitor = new CollectingVisitor<HRegionInfo>() {
      private HRegionInfo current = null;

      @Override
      public boolean visit(Result r) throws IOException {
        this.current = HRegionInfo.getHRegionInfo(r, HConstants.REGIONINFO_QUALIFIER);
        if (this.current == null) {
          LOG.warn("No serialized HRegionInfo in " + r);
          return true;
        }
        if (!isInsideTable(this.current, tableName)) return false;
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
    fullScan(conn, visitor, getTableStartRowForMeta(tableName));
    // If visitor has results >= 1 then table exists.
    return visitor.getResults().size() >= 1;
  }

  /**
   * @param current
   * @param tableName
   * @return True if <code>current</code> tablename is equal to
   * <code>tableName</code>
   */
  static boolean isInsideTable(final HRegionInfo current, final TableName tableName) {
    return tableName.equals(current.getTable());
  }

  /**
   * @param tableName
   * @return Place to start Scan in <code>hbase:meta</code> when passed a
   * <code>tableName</code>; returns &lt;tableName&rt; &lt;,&rt; &lt;,&rt;
   */
  static byte [] getTableStartRowForMeta(TableName tableName) {
    byte [] startRow = new byte[tableName.getName().length + 2];
    System.arraycopy(tableName.getName(), 0, startRow, 0, tableName.getName().length);
    startRow[startRow.length - 2] = HConstants.DELIMITER;
    startRow[startRow.length - 1] = HConstants.DELIMITER;
    return startRow;
  }

  /**
   * This method creates a Scan object that will only scan catalog rows that
   * belong to the specified table. It doesn't specify any columns.
   * This is a better alternative to just using a start row and scan until
   * it hits a new table since that requires parsing the HRI to get the table
   * name.
   * @param tableName bytes of table's name
   * @return configured Scan object
   */
  public static Scan getScanForTableName(TableName tableName) {
    String strName = tableName.getNameAsString();
    // Start key is just the table name with delimiters
    byte[] startKey = Bytes.toBytes(strName + ",,");
    // Stop key appends the smallest possible char to the table name
    byte[] stopKey = Bytes.toBytes(strName + " ,,");
    return new Scan().withStartRow(startKey).withStopRow(stopKey);
  }

  /**
   * @param conn
   * @param serverName
   * @return List of user regions installed on this server (does not include
   * catalog regions).
   * @throws IOException
   */
  public static NavigableMap<HRegionInfo, Result> getServerUserRegions(HConnection conn,
      final ServerName serverName) throws IOException {
    final NavigableMap<HRegionInfo, Result> hris = new TreeMap<HRegionInfo, Result>();
    // Fill the above hris map with entries from hbase:meta that have the passed
    // servername.
    CollectingVisitor<Result> v = new CollectingVisitor<Result>() {
      @Override
      void add(Result r) {
        if (r == null || r.isEmpty()) return;
        if (HRegionInfo.getHRegionInfo(r) == null) return;
        ServerName sn = HRegionInfo.getServerName(r);
        if (sn != null && sn.equals(serverName)) {
          this.results.add(r);
        }
      }
    };
    fullScan(conn, v);
    List<Result> results = v.getResults();
    if (results != null && !results.isEmpty()) {
      // Convert results to Map keyed by HRI
      for (Result r : results) {
        HRegionInfo hri = HRegionInfo.getHRegionInfo(r);
        if (hri != null) hris.put(hri, r);
      }
    }
    return hris;
  }

  public static void fullScanMetaAndPrint(HConnection conn) throws IOException {
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) return true;
        LOG.info("fullScanMetaAndPrint.Current Meta Row: " + r);
        HRegionInfo hrim = HRegionInfo.getHRegionInfo(r);
        LOG.info("fullScanMetaAndPrint.HRI Print= " + hrim);
        return true;
      }
    };
    fullScan(conn, v);
  }

  /**
   * Performs a full scan of a catalog table.
   * @param conn
   * @param visitor Visitor invoked against each row.
   * @param startrow Where to start the scan. Pass null if want to begin scan
   * at first row.
   * <code>hbase:meta</code>, the default (pass false to scan hbase:meta)
   * @throws IOException
   */
  public static void fullScan(HConnection conn, Visitor visitor, byte[] startrow)
      throws IOException {
    Scan scan = new Scan();
    if (startrow != null) {
      scan.withStartRow(startrow);
    }
    if (startrow == null) {
      int caching = conn.getConfiguration().getInt(HConstants.HBASE_META_SCANNER_CACHING, 100);
      scan.setCaching(caching);
    }
    scan.addFamily(HConstants.CATALOG_FAMILY);
    HTable metaTable = getMetaHTable(conn);
    ResultScanner scanner = null;
    try {
      scanner = metaTable.getScanner(scan);
      Result data;
      while ((data = scanner.next()) != null) {
        if (data.isEmpty()) continue;
        // Break if visit returns false.
        if (!visitor.visit(data)) break;
      }
    } finally {
      if (scanner != null) scanner.close();
      metaTable.close();
    }
    return;
  }

  /**
   * Implementations 'visit' a catalog table row.
   */
  public interface Visitor {
    /**
     * Visit the catalog table row.
     * @param r A row from catalog table
     * @return True if we are to proceed scanning the table, else false if
     * we are to stop now.
     */
    boolean visit(final Result r) throws IOException;
  }

  /**
   * A {@link Visitor} that collects content out of passed {@link Result}.
   */
  static abstract class CollectingVisitor<T> implements Visitor {
    final List<T> results = new ArrayList<T>();
    @Override
    public boolean visit(Result r) throws IOException {
      if (r ==  null || r.isEmpty()) return true;
      add(r);
      return true;
    }

    abstract void add(Result r);

    /**
     * @return Collected results; wait till visits complete to collect all
     * possible results
     */
    List<T> getResults() {
      return this.results;
    }
  }

  /**
   * Collects all returned.
   */
  static class CollectAllVisitor extends CollectingVisitor<Result> {
    @Override
    void add(Result r) {
      this.results.add(r);
    }
  }

  /**
   * Count regions in <code>hbase:meta</code> for passed table.
   * @param c
   * @param tableName
   * @return Count or regions in table <code>tableName</code>
   * @throws IOException
   */
  public static int getRegionCount(final Configuration c, final String tableName) throws IOException {
    HTable t = new HTable(c, tableName);
    try {
      return t.getRegionLocations().size();
    } finally {
      t.close();
    }
  }

  /**
   * Decode table state from META Result. Should contain cell from HConstants.TABLE_FAMILY
   * @param r result
   * @return null if not found
   * @throws IOException
   */
  @Nullable
  public static TableState getTableState(Result r) throws IOException {
    Cell cell = r.getColumnLatestCell(HConstants.TABLE_FAMILY, HConstants.TABLE_STATE_QUALIFIER);
    if (cell == null) {
      return null;
    }
    try {
      return TableState.parseFrom(TableName.valueOf(r.getRow()),
        Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(),
          cell.getValueOffset() + cell.getValueLength()));
    } catch (DeserializationException e) {
      throw new IOException(e);
    }
  }

  @Nullable
  public static TableState getTableState(HConnection conn, TableName tableName) throws IOException {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      return new TableState(tableName, TableState.State.ENABLED);
    }
    try (HTable metaHTable = getMetaHTable(conn)) {
      Get get = new Get(tableName.getName()).addColumn(HConstants.TABLE_FAMILY,
        HConstants.TABLE_STATE_QUALIFIER);
      Result result = metaHTable.get(get);
      return getTableState(result);
    }
  }
}

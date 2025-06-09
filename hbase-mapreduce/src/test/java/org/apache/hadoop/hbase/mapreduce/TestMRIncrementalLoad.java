/**
 * 
 */
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@Category({ MapReduceTests.class, LargeTests.class })
public class TestMRIncrementalLoad extends HFileOutputFormat2TestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestMRIncrementalLoad.class);

  @Parameter(0)
  public boolean shouldChangeRegions;

  @Parameter(1)
  public boolean shouldKeepLocality;

  @Parameter(2)
  public boolean putSortReducer;

  @Parameter(3)
  public List<String> tableStr;

  @Parameters(name = "{index}: shouldChangeRegions={0}, shouldKeepLocality={1}, putSortReducer={2},"
    + " tableStr={3}")
  public static List<Object[]> params() {
    return Arrays.asList(
      new Object[] { false, false, false, Arrays.asList("testMRIncrementalLoad") },
      new Object[] { true, false, false, Arrays.asList("testMRIncrementalLoadWithSplit") },
      new Object[] { false, true, false, Arrays.asList("testMRIncrementalLoadWithSplit") },
      new Object[] { true, true, false, Arrays.asList("testMRIncrementalLoadWithLocality1") },
      new Object[] { true, false, false, Arrays.asList("testMRIncrementalLoadWithLocality2") },
      new Object[] { false, false, true, Arrays.asList("testMRIncrementalLoadWithPutSortReducer") },
      new Object[] { false, false, true,
        Arrays.stream(TABLE_NAMES).map(TableName::getNameAsString).collect(Collectors.toList()) });
  }

  private String[] hostnames;

  private Map<String, Table> allTables;

  private List<HFileOutputFormat2.TableInfo> tableInfo;

  private Path testDir;

  @Before
  public void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(MultiTableHFileOutputFormat.LOCALITY_SENSITIVE_CONF_KEY, shouldKeepLocality);
    int hostCount = 1;
    int regionNum = 5;
    if (shouldKeepLocality) {
      // We should change host count higher than hdfs replica count when MiniHBaseCluster supports
      // explicit hostnames parameter just like MiniDFSCluster does.
      hostCount = 3;
      regionNum = 20;
    }

    hostnames = new String[hostCount];
    for (int i = 0; i < hostCount; ++i) {
      hostnames[i] = "datanode_" + i;
    }
    StartTestingClusterOption option = StartTestingClusterOption.builder()
      .numRegionServers(hostCount).dataNodeHosts(hostnames).build();
    UTIL.getConfiguration().unset(HConstants.TEMPORARY_FS_DIRECTORY_KEY);
    UTIL.startMiniCluster(option);

    allTables = new HashMap<>(tableStr.size());
    tableInfo = new ArrayList<>(tableStr.size());
    for (String tableStrSingle : tableStr) {
      byte[][] splitKeys = generateRandomSplitKeys(regionNum - 1);
      TableName tableName = TableName.valueOf(tableStrSingle);
      Table table = UTIL.createTable(tableName, FAMILIES, splitKeys);

      RegionLocator r = UTIL.getConnection().getRegionLocator(tableName);
      assertEquals("Should start with empty table", 0, HBaseTestingUtil.countRows(table));
      int numRegions = r.getStartKeys().length;
      assertEquals("Should make " + regionNum + " regions", numRegions, regionNum);

      allTables.put(tableStrSingle, table);
      tableInfo.add(new HFileOutputFormat2.TableInfo(table.getDescriptor(), r));
    }
    testDir = UTIL.getDataTestDirOnTestFS(getClass().getSimpleName());
  }

  @After
  public void tearDown() throws IOException {
    for (HFileOutputFormat2.TableInfo tableInfoSingle : tableInfo) {
      tableInfoSingle.getRegionLocator().close();
    }
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void doIncrementalLoadTest() throws Exception {
    boolean writeMultipleTables = tableStr.size() > 1;
    // Generate the bulk load files
    runIncrementalPELoad(UTIL.getConfiguration(), tableInfo, testDir, putSortReducer);
    if (writeMultipleTables) {
      testDir = new Path(testDir, "default");
    }

    for (Table tableSingle : allTables.values()) {
      // This doesn't write into the table, just makes files
      assertEquals("HFOF should not touch actual table", 0,
        HBaseTestingUtil.countRows(tableSingle));
    }
    int numTableDirs = 0;
    FileStatus[] fss = testDir.getFileSystem(UTIL.getConfiguration()).listStatus(testDir);
    for (FileStatus tf : fss) {
      Path tablePath = testDir;
      if (writeMultipleTables) {
        if (allTables.containsKey(tf.getPath().getName())) {
          ++numTableDirs;
          tablePath = tf.getPath();
        } else {
          continue;
        }
      }

      // Make sure that a directory was created for every CF
      int dir = 0;
      fss = tablePath.getFileSystem(UTIL.getConfiguration()).listStatus(tablePath);
      for (FileStatus f : fss) {
        for (byte[] family : FAMILIES) {
          if (Bytes.toString(family).equals(f.getPath().getName())) {
            ++dir;
          }
        }
      }
      assertEquals("Column family not found in FS.", FAMILIES.length, dir);
    }
    if (writeMultipleTables) {
      assertEquals("Dir for all input tables not created", numTableDirs, allTables.size());
    }

    Admin admin = UTIL.getAdmin();

    // handle the split case
    if (shouldChangeRegions) {
      Table chosenTable = allTables.values().iterator().next();
      // Choose a semi-random table if multiple tables are available
      LOG.info("Changing regions in table " + chosenTable.getName().getNameAsString());
      admin.disableTable(chosenTable.getName());
      UTIL.waitUntilNoRegionsInTransition();

      UTIL.deleteTable(chosenTable.getName());
      byte[][] newSplitKeys = generateRandomSplitKeys(14);
      UTIL.createTable(chosenTable.getName(), FAMILIES, newSplitKeys);
      UTIL.waitTableAvailable(chosenTable.getName());
    }

    // Perform the actual load
    for (HFileOutputFormat2.TableInfo singleTableInfo : tableInfo) {
      Path tableDir = testDir;
      String tableNameStr = singleTableInfo.getTableDescriptor().getTableName().getNameAsString();
      LOG.info("Running BulkLoadHFiles on table" + tableNameStr);
      if (writeMultipleTables) {
        tableDir = new Path(testDir, tableNameStr);
      }
      Table currentTable = allTables.get(tableNameStr);
      TableName currentTableName = currentTable.getName();
      BulkLoadHFiles.create(UTIL.getConfiguration()).bulkLoad(currentTableName, tableDir);

      // Ensure data shows up
      int expectedRows = 0;
      if (putSortReducer) {
        // no rows should be extracted
        assertEquals("BulkLoadHFiles should put expected data in table", expectedRows,
          HBaseTestingUtil.countRows(currentTable));
      } else {
        expectedRows = NMapInputFormat.getNumMapTasks(UTIL.getConfiguration()) * ROWSPERSPLIT;
        assertEquals("BulkLoadHFiles should put expected data in table", expectedRows,
          HBaseTestingUtil.countRows(currentTable));
        Scan scan = new Scan();
        ResultScanner results = currentTable.getScanner(scan);
        for (Result res : results) {
          assertEquals(FAMILIES.length, res.rawCells().length);
          Cell first = res.rawCells()[0];
          for (Cell kv : res.rawCells()) {
            assertTrue(CellUtil.matchingRows(first, kv));
            assertTrue(Bytes.equals(CellUtil.cloneValue(first), CellUtil.cloneValue(kv)));
          }
        }
        results.close();
      }
      String tableDigestBefore = UTIL.checksumRows(currentTable);
      // Check region locality
      HDFSBlocksDistribution hbd = new HDFSBlocksDistribution();
      for (HRegion region : UTIL.getHBaseCluster().getRegions(currentTableName)) {
        hbd.add(region.getHDFSBlocksDistribution());
      }
      for (String hostname : hostnames) {
        float locality = hbd.getBlockLocalityIndex(hostname);
        LOG.info("locality of [" + hostname + "]: " + locality);
        assertEquals(100, (int) (locality * 100));
      }

      // Cause regions to reopen
      admin.disableTable(currentTableName);
      while (!admin.isTableDisabled(currentTableName)) {
        Thread.sleep(200);
        LOG.info("Waiting for table to disable");
      }
      admin.enableTable(currentTableName);
      UTIL.waitTableAvailable(currentTableName);
      assertEquals("Data should remain after reopening of regions", tableDigestBefore,
        UTIL.checksumRows(currentTable));
    }
  }
}

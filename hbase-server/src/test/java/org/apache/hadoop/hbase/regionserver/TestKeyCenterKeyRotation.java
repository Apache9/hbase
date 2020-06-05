/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.Key;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.xiaomi.infra.crypto.KeyCenterKeyProvider;

@Category(MediumTests.class)
public class TestKeyCenterKeyRotation {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestKeyCenterKeyRotation.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final Configuration conf = TEST_UTIL.getConfiguration();

  private static final byte[] CF = Bytes.toBytes("CF");

  private static final byte[] CQ = Bytes.toBytes("CQ");

  private static final int HFILE_NUM = 5;

  private static final int COUNT = 100;

  private static final TableName tableName = TableName.valueOf("TestKeyCenterKeyRotation");

  private static final String initialKey;
  private static final String secondKey;
  private static final String initialWrappedKey;
  private static final String secondWrappedKey;

  static {
    SecureRandom rng = new SecureRandom();
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    byte[] wrappedKeyBytes = new byte[256];
    rng.nextBytes(keyBytes);
    initialKey = Bytes.toStringBinary(keyBytes);
    rng.nextBytes(wrappedKeyBytes);
    initialWrappedKey = Bytes.toStringBinary(wrappedKeyBytes);
    rng.nextBytes(keyBytes);
    secondKey = Bytes.toStringBinary(keyBytes);
    rng.nextBytes(wrappedKeyBytes);
    secondWrappedKey = Bytes.toStringBinary(wrappedKeyBytes);
    // Add to cache directly as the unit test can't access keycenter service
    KeyCenterKeyProvider.addToCache(initialKey, initialWrappedKey);
    KeyCenterKeyProvider.addToCache(secondKey, secondWrappedKey);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    conf.setInt("hfile.format.version", 3);
    // Store the wrapped key to config
    conf.set(KeyCenterKeyProvider.CRYPTO_KEYCENTER_KEY, initialWrappedKey);
    // Skip load cache as the unit test can't access keycenter service
    conf.setBoolean(KeyCenterKeyProvider.SKIP_ACCESS_KEYCENTER, true);
    // Start the minicluster
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testKeyCenterKeyRotation() throws Exception {
    // Create the table schema
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    ColumnFamilyDescriptor hcd = ColumnFamilyDescriptorBuilder.newBuilder(CF).setEncryptionType(algorithm).build();
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(hcd).build();

    TEST_UTIL.getAdmin().createTable(htd);
    TEST_UTIL.waitTableAvailable(htd.getTableName(), 5000);
    loadData();

    // Verify we have store file(s) with the initial key
    List<Path> storeFilePaths = findStorefilePaths(htd.getTableName());
    assertTrue(storeFilePaths.size() > 0);
    for (Path path : storeFilePaths) {
      assertTrue("Store file " + path + " has incorrect key",
        Bytes.equals(Bytes.toBytesBinary(initialKey), extractHFileKey(path)));
    }
    verifyData();

    // Now shut down the HBase cluster
    TEST_UTIL.shutdownMiniHBaseCluster();

    // "Rotate" the keycenter key
    conf.set(KeyCenterKeyProvider.CRYPTO_KEYCENTER_KEY, secondWrappedKey);
    TEST_UTIL.startMiniHBaseCluster(1, 1);
    // Verify the table can still be loaded
    TEST_UTIL.waitTableAvailable(htd.getTableName(), 5000);
    TEST_UTIL.getAdmin().majorCompact(tableName);

    // Wait the major compact to finish
    do {
      Thread.sleep(1000);
      storeFilePaths = findStorefilePaths(htd.getTableName());
    } while (storeFilePaths.size() != 1);
    assertTrue("Store file " + storeFilePaths.get(0) + " has incorrect key",
      Bytes.equals(Bytes.toBytesBinary(secondKey), extractHFileKey(storeFilePaths.get(0))));
    verifyData();
  }

  private static List<Path> findStorefilePaths(TableName tableName) throws Exception {
    List<Path> paths = new ArrayList<Path>();
    for (HRegion region : TEST_UTIL.getRSForFirstRegionInTable(tableName)
        .getRegions(tableName)) {
      for (Store store : region.getStores()) {
        for (StoreFile storefile : store.getStorefiles()) {
          paths.add(storefile.getPath());
        }
      }
    }
    return paths;
  }

  private void loadData() throws Exception {
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < HFILE_NUM; i++) {
        for (int j = 0; j < COUNT; j++) {
          byte[] row = Bytes.toBytes(i * COUNT + j);
          table.put(new Put(row).add(new KeyValue(row, CF, CQ, row)));
        }
        TEST_UTIL.getAdmin().flush(tableName);
      }
    }
  }

  private void verifyData() throws Exception {
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < HFILE_NUM * COUNT; i++) {
        assertEquals(i, Bytes.toInt(table.get(new Get(Bytes.toBytes(i))).getValue(CF, CQ)));
      }
    }
  }

  private static byte[] extractHFileKey(Path path) throws Exception {
    HFile.Reader reader =
        HFile.createReader(TEST_UTIL.getTestFileSystem(), path, new CacheConfig(conf), true, conf);
    try {
      Encryption.Context cryptoContext = reader.getFileContext().getEncryptionContext();
      assertNotNull("Reader has a null crypto context", cryptoContext);
      Key key = cryptoContext.getKey();
      assertNotNull("Crypto context has no key", key);
      return key.getEncoded();
    } finally {
      reader.close();
    }
  }

}

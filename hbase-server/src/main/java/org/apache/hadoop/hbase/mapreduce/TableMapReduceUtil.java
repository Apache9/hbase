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
package org.apache.hadoop.hbase.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.xiaomi.infra.base.nameservice.NameService;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.JarFinder;
import org.apache.hadoop.hbase.mapreduce.salted.SaltedMultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.salted.SaltedTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.salted.SaltedTableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBuf;

/**
 * Utility for {@link TableMapper} and {@link TableReducer}
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TableMapReduceUtil {
  static Log LOG = LogFactory.getLog(TableMapReduceUtil.class);

  /**
   * Use this before submitting a TableMap job. It will appropriately set up
   * the job.
   *
   * @param table  The table name to read from.
   * @param scan  The scan instance with the columns, time range etc.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @throws IOException When setting up the details fails.
   */
  public static void initTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job)
  throws IOException {
    initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass,
        job, true);
  }

  /**
   * Use this before submitting a TableMap job. It will appropriately set up
   * the job.
   *
   * @param table Binary representation of the table name to read from.
   * @param scan  The scan instance with the columns, time range etc.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @throws IOException When setting up the details fails.
   */
   public static void initTableMapperJob(byte[] table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job)
  throws IOException {
      initTableMapperJob(Bytes.toString(table), scan, mapper, outputKeyClass, outputValueClass,
              job, true);
  }

   /**
    * Use this before submitting a TableMap job. It will appropriately set up
    * the job.
    *
    * @param table  The table name to read from.
    * @param scan  The scan instance with the columns, time range etc.
    * @param mapper  The mapper class to use.
    * @param outputKeyClass  The class of the output key.
    * @param outputValueClass  The class of the output value.
    * @param job  The current job to adjust.  Make sure the passed job is
    * carrying all necessary HBase configuration.
    * @param addDependencyJars upload HBase jars and jars for any of the configured
    *           job classes via the distributed cache (tmpjars).
    * @throws IOException When setting up the details fails.
    */
   public static void initTableMapperJob(String table, Scan scan,
       Class<? extends TableMapper> mapper,
       Class<?> outputKeyClass,
       Class<?> outputValueClass, Job job,
       boolean addDependencyJars, Class<? extends InputFormat> inputFormatClass)
   throws IOException {
     initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass, job,
         addDependencyJars, true, inputFormatClass);
   }


  /**
   * Use this before submitting a TableMap job. It will appropriately set up
   * the job.
   *
   * @param table  The table name to read from.
   * @param scan  The scan instance with the columns, time range etc.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   * @param initCredentials whether to initialize hbase auth credentials for the job
   * @param inputFormatClass the input format
   * @throws IOException When setting up the details fails.
   */
  public static void initTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job,
      boolean addDependencyJars, boolean initCredentials,
      Class<? extends InputFormat> inputFormatClass)
  throws IOException {
    job.setInputFormatClass(inputFormatClass);
    if (outputValueClass != null) job.setMapOutputValueClass(outputValueClass);
    if (outputKeyClass != null) job.setMapOutputKeyClass(outputKeyClass);
    job.setMapperClass(mapper);
    if (Put.class.equals(outputValueClass)) {
      job.setCombinerClass(PutCombiner.class);
    }
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
    conf.set(TableInputFormat.INPUT_TABLE, table);
    conf.set(TableInputFormat.SCAN, convertScanToString(scan));
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        MutationSerialization.class.getName(), ResultSerialization.class.getName(),
        KeyValueSerialization.class.getName());
    if (addDependencyJars) {
      addDependencyJars(job);
    }
    if (initCredentials) {
      initCredentials(job);
    }
  }

  /**
   * Use this before submitting a TableMap job. It will appropriately set up
   * the job.
   *
   * @param table Binary representation of the table name to read from.
   * @param scan  The scan instance with the columns, time range etc.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   * @param inputFormatClass The class of the input format
   * @throws IOException When setting up the details fails.
   */
  public static void initTableMapperJob(byte[] table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job,
      boolean addDependencyJars, Class<? extends InputFormat> inputFormatClass)
  throws IOException {
      initTableMapperJob(Bytes.toString(table), scan, mapper, outputKeyClass,
              outputValueClass, job, addDependencyJars, inputFormatClass);
  }

  /**
   * Use this before submitting a TableMap job. It will appropriately set up
   * the job.
   *
   * @param table Binary representation of the table name to read from.
   * @param scan  The scan instance with the columns, time range etc.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   * @throws IOException When setting up the details fails.
   */
  public static void initTableMapperJob(byte[] table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job,
      boolean addDependencyJars)
  throws IOException {
    initTableMapperJob(Bytes.toString(table), scan, mapper, outputKeyClass, outputValueClass, job,
      addDependencyJars, getTableInputFormatCls(job.getConfiguration(), table));
  }

  /**
   * Use this before submitting a TableMap job. It will appropriately set up
   * the job.
   *
   * @param table The table name to read from.
   * @param scan  The scan instance with the columns, time range etc.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   * @throws IOException When setting up the details fails.
   */
  public static void initTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job,
      boolean addDependencyJars)
  throws IOException {
    initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass, job,
      addDependencyJars, getTableInputFormatCls(job.getConfiguration(), Bytes.toBytes(table)));
  }
  
  protected static Class<? extends MultiTableInputFormat> getMultiTableInputFormatCls(
      Configuration conf, List<Scan> scans) throws IOException {
    for (Scan scan : scans) {
      byte[] tableName = scan.getAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME);
      if (tableName == null) throw new IOException("A scan object did not have a table name");
      if (SaltedTableMapReduceUtil.isSaltedTable(conf, tableName)) {
        return SaltedMultiTableInputFormat.class;

      }

    }
    return MultiTableInputFormat.class;
  }

  protected static Class<? extends TableInputFormat> getTableInputFormatCls(Configuration conf,
      byte[] tableName) throws IOException {
    return SaltedTableMapReduceUtil.isSaltedTable(conf, tableName) ? SaltedTableInputFormat.class
        : TableInputFormat.class;
  }

  /**
   * Enable a basic on-heap cache for these jobs. Any BlockCache implementation based on
   * direct memory will likely cause the map tasks to OOM when opening the region. This
   * is done here instead of in TableSnapshotRegionRecordReader in case an advanced user
   * wants to override this behavior in their job.
   */
  public static void resetCacheConfig(Configuration conf) {
    conf.setFloat(
      HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
    conf.setFloat("hbase.offheapcache.percentage", 0f);
    conf.setFloat("hbase.bucketcache.size", 0f);
    conf.unset("hbase.bucketcache.ioengine");
  }

  /**
   * Sets up the job for reading from a table snapshot. It bypasses hbase servers
   * and read directly from snapshot files.
   *
   * @param snapshotName The name of the snapshot (of a table) to read from.
   * @param scan  The scan instance with the columns, time range etc.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   *
   * @param tmpRestoreDir a temporary directory to copy the snapshot files into. Current user should
   * have write permissions to this directory, and this should not be a subdirectory of rootdir.
   * After the job is finished, restore directory can be deleted.
   * @throws IOException When setting up the details fails.
   * @see TableSnapshotInputFormat
   */
  public static void initTableSnapshotMapperJob(String snapshotName, Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job,
      boolean addDependencyJars, Path tmpRestoreDir)
  throws IOException {
    checkUmask(job.getConfiguration());
    TableSnapshotInputFormat.setInput(job, snapshotName, tmpRestoreDir);
    initTableMapperJob(snapshotName, scan, mapper, outputKeyClass,
        outputValueClass, job, addDependencyJars, true, TableSnapshotInputFormat.class);
    addDependencyJars(job.getConfiguration(), MetricsRegistry.class, ByteBuf.class);
    resetCacheConfig(job.getConfiguration());
  }

  private static void checkUmask(Configuration conf) throws IOException {
    String umask = conf.get("fs.permissions.umask-mode");
    if (umask == null || !(umask.equals("007") || umask.equals("003") || umask.equals("002")
        || umask.equals("006") || umask.equals("000"))) {
      throw new DoNotRetryIOException(
          "Please set fs.permissions.umask-mode to 007, current umask: " + umask);
    }
  }

  @VisibleForTesting
  static Path getAndCheckTmpRestoreDir(Configuration conf) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path tmpRestoreDir = new Path(rootDir.toUri().getScheme(), rootDir.toUri().getAuthority(),
        conf.get(HConstants.SNAPSHOT_RESTORE_TMP_DIR, HConstants.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT));
    if (!fs.exists(tmpRestoreDir)) {
      throw new IOException("tmp directory to restore snapshot does not exist. " + tmpRestoreDir);
    }
    return tmpRestoreDir;
  }

  public static String initTableSnapshotMapperJob(TableName table, Scan scan,
      Class<? extends TableMapper> mapper, Class<?> outputKeyClass, Class<?> outputValueClass,
      Job job, boolean addDependencyJars) throws IOException {
    Configuration conf = job.getConfiguration();
    Path tmpRestoreDir = getAndCheckTmpRestoreDir(conf);
    return initTableSnapshotMapperJob(table, scan, mapper, outputKeyClass, outputValueClass, job,
      addDependencyJars, tmpRestoreDir);
  }

  /**
   * Sets up the job for reading from a table snapshot. It bypasses hbase servers and read directly
   * from snapshot files. This method will create a snapshot with TTL(ms), and user must make sure
   * that the job can be finished before the snapshot is deleted.
   * @param table The name of a table to read from.
   * @param scan The scan instance with the columns, time range etc.
   * @param mapper The mapper class to use.
   * @param outputKeyClass The class of the output key.
   * @param outputValueClass The class of the output value.
   * @param job The current job to adjust. Make sure the passed job is carrying all necessary HBase
   *          configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured job classes via
   *          the distributed cache (tmpjars).
   * @param ttl TTL of the snapshot, in milliseconds. 0 means forever.
   * @return The snapshot name.
   * @throws IOException When setting up the details fails.
   */
  public static String initTableSnapshotMapperJob(TableName table, Scan scan,
      Class<? extends TableMapper> mapper, Class<?> outputKeyClass, Class<?> outputValueClass,
      Job job, boolean addDependencyJars, long ttl) throws IOException {
    Configuration conf = job.getConfiguration();
    Path tmpRestoreDir = getAndCheckTmpRestoreDir(conf);
    return initTableSnapshotMapperJob(table, scan, mapper, outputKeyClass, outputValueClass, job,
      addDependencyJars, tmpRestoreDir, ttl);
  }

  public static String initTableSnapshotMapperJob(TableName table, Scan scan,
      Class<? extends TableMapper> mapper, Class<?> outputKeyClass, Class<?> outputValueClass,
      Job job, boolean addDependencyJars, Path tmpRestoreDir) throws IOException {
    checkUmask(job.getConfiguration());
    // Generate a snapshot name, based on namespace, table name, local timestamp.
    String snapshotName = String.format("SnapshotInputFormat-%s-%s-%d",
      table.getNamespaceAsString(), table.getQualifierAsString(), System.currentTimeMillis());

    // Create a snapshot.
    try (HBaseAdmin admin = new HBaseAdmin(job.getConfiguration())) {
      admin.snapshot(snapshotName, table, Type.SKIPFLUSH);
    }

    // Initialize the snapshot mapper job.
    initTableSnapshotMapperJob(snapshotName, scan, mapper, outputKeyClass, outputValueClass, job,
      addDependencyJars, tmpRestoreDir);
    return snapshotName;
  }

  /**
   * Sets up the job for reading from a table snapshot. It bypasses hbase servers and read directly
   * from snapshot files. This method will create a snapshot with TTL(ms), and user must make sure
   * that the job can be finished before the snapshot is deleted.
   * @param table The name of a table to read from.
   * @param scan The scan instance with the columns, time range etc.
   * @param mapper The mapper class to use.
   * @param outputKeyClass The class of the output key.
   * @param outputValueClass The class of the output value.
   * @param job The current job to adjust. Make sure the passed job is carrying all necessary HBase
   *          configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured job classes via
   *          the distributed cache (tmpjars).
   * @param tmpRestoreDir The directory to restore the snapshot.
   * @param ttl TTL of the snapshot, in milliseconds. 0 means forever.
   * @return The snapshot name.
   * @throws IOException When setting up the details fails.
   */
  public static String initTableSnapshotMapperJob(TableName table, Scan scan,
      Class<? extends TableMapper> mapper, Class<?> outputKeyClass, Class<?> outputValueClass,
      Job job, boolean addDependencyJars, Path tmpRestoreDir, long ttl) throws IOException {
    checkUmask(job.getConfiguration());
    // Generate a snapshot name, based on namespace, table name, local timestamp.
    String snapshotName = String.format("SnapshotInputFormat-%s-%s-%d",
      table.getNamespaceAsString(), table.getQualifierAsString(), System.currentTimeMillis());

    // Create a snapshot.
    try (HBaseAdmin admin = new HBaseAdmin(job.getConfiguration())) {
      admin.snapshot(snapshotName, table, Type.SKIPFLUSH, ttl);
    }

    // Initialize the snapshot mapper job.
    initTableSnapshotMapperJob(snapshotName, scan, mapper, outputKeyClass, outputValueClass, job,
      addDependencyJars, tmpRestoreDir);
    return snapshotName;
  }

  /**
   * Sets up the job for reading from one or more multiple table snapshots, with one or more scan
   * per snapshot. It bypasses hbase servers and read directly from snapshot files.
   * @param snapshotScans map of snapshot name to scans on that snapshot.
   * @param mapper The mapper class to use.
   * @param outputKeyClass The class of the output key.
   * @param outputValueClass The class of the output value.
   * @param job The current job to adjust. Make sure the passed job is carrying all necessary HBase
   *          configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured job classes via
   *          the distributed cache (tmpjars).
   */
  public static void initMultiTableSnapshotMapperJob(Map<String, Collection<Scan>> snapshotScans,
      Class<? extends TableMapper> mapper, Class<?> outputKeyClass, Class<?> outputValueClass,
      Job job, boolean addDependencyJars, Path tmpRestoreDir) throws IOException {
    MultiTableSnapshotInputFormat.setInput(job.getConfiguration(), snapshotScans, tmpRestoreDir);

    job.setInputFormatClass(MultiTableSnapshotInputFormat.class);
    if (outputValueClass != null) {
      job.setMapOutputValueClass(outputValueClass);
    }
    if (outputKeyClass != null) {
      job.setMapOutputKeyClass(outputKeyClass);
    }
    job.setMapperClass(mapper);
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));

    if (addDependencyJars) {
      addDependencyJars(job);
      addDependencyJars(job.getConfiguration(), MetricsRegistry.class);
    }

    resetCacheConfig(job.getConfiguration());
  }

  public static void cleanupTableSnapshotMapperJob(Job job, String snapshotName)
      throws IOException {
    Configuration conf = job.getConfiguration();
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      admin.deleteSnapshot(snapshotName);
      Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
      FileSystem fs = rootDir.getFileSystem(conf);
      String restoreDirPath = conf.get(TableSnapshotInputFormatImpl.RESTORE_DIR_KEY);
      Path restoreDir = new Path(restoreDirPath);
      if (fs.exists(restoreDir)) {
        LOG.info("Delete restore dir " + restoreDirPath);
        fs.delete(restoreDir, true);
      }
    }
  }

  /**
   * Use this before submitting a Multi TableMap job. It will appropriately set
   * up the job.
   *
   * @param scans The list of {@link Scan} objects to read from.
   * @param mapper The mapper class to use.
   * @param outputKeyClass The class of the output key.
   * @param outputValueClass The class of the output value.
   * @param job The current job to adjust. Make sure the passed job is carrying
   *          all necessary HBase configuration.
   * @throws IOException When setting up the details fails.
   */
  public static void initTableMapperJob(List<Scan> scans,
      Class<? extends TableMapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<? extends Writable> outputValueClass, Job job) throws IOException {
    initTableMapperJob(scans, mapper, outputKeyClass, outputValueClass, job,
        true);
  }

  /**
   * Use this before submitting a Multi TableMap job. It will appropriately set
   * up the job.
   *
   * @param scans The list of {@link Scan} objects to read from.
   * @param mapper The mapper class to use.
   * @param outputKeyClass The class of the output key.
   * @param outputValueClass The class of the output value.
   * @param job The current job to adjust. Make sure the passed job is carrying
   *          all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the
   *          configured job classes via the distributed cache (tmpjars).
   * @throws IOException When setting up the details fails.
   */
  public static void initTableMapperJob(List<Scan> scans,
      Class<? extends TableMapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<? extends Writable> outputValueClass, Job job,
      boolean addDependencyJars) throws IOException {
    initTableMapperJob(scans, mapper, outputKeyClass, outputValueClass, job,
      addDependencyJars, true);
  }

    /**
   * Sets up the job for reading from a table snapshot. It bypasses hbase servers
   * and read directly from snapshot files.
   *
   * @param snapshotName The name of the snapshot (of a table) to read from.
   * @param scan  The scan instance with the columns, time range etc.
   * @param mapper  The mapper class to use.
   * @param outputKeyClass  The class of the output key.
   * @param outputValueClass  The class of the output value.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   *
   * @param tmpRestoreDir a temporary directory to copy the snapshot files into. Current user should
   * have write permissions to this directory, and this should not be a subdirectory of rootdir.
   * After the job is finished, restore directory can be deleted.
   * @param splitAlgo algorithm to split
   * @param numSplitsPerRegion how many input splits to generate per one region
   * @throws IOException When setting up the details fails.
   * @see TableSnapshotInputFormat
   */
  public static void initTableSnapshotMapperJob(String snapshotName, Scan scan,
                                                Class<? extends TableMapper> mapper,
                                                Class<?> outputKeyClass,
                                                Class<?> outputValueClass, Job job,
                                                boolean addDependencyJars, Path tmpRestoreDir,
                                                RegionSplitter.SplitAlgorithm splitAlgo,
                                                int numSplitsPerRegion)
          throws IOException {
    checkUmask(job.getConfiguration());
    TableSnapshotInputFormat.setInput(job, snapshotName, tmpRestoreDir, splitAlgo,
            numSplitsPerRegion);
    initTableMapperJob(snapshotName, scan, mapper, outputKeyClass,
            outputValueClass, job, addDependencyJars, false, TableSnapshotInputFormat.class);
    resetCacheConfig(job.getConfiguration());
  }


  /**
   * Use this before submitting a Multi TableMap job. It will appropriately set
   * up the job.
   *
   * @param scans The list of {@link Scan} objects to read from.
   * @param mapper The mapper class to use.
   * @param outputKeyClass The class of the output key.
   * @param outputValueClass The class of the output value.
   * @param job The current job to adjust. Make sure the passed job is carrying
   *          all necessary HBase configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the
   *          configured job classes via the distributed cache (tmpjars).
   * @param initCredentials whether to initialize hbase auth credentials for the job
   * @throws IOException When setting up the details fails.
   */
  public static void initTableMapperJob(List<Scan> scans,
      Class<? extends TableMapper> mapper,
      Class<? extends WritableComparable> outputKeyClass,
      Class<? extends Writable> outputValueClass, Job job,
      boolean addDependencyJars,
      boolean initCredentials) throws IOException {
    job.setInputFormatClass(getMultiTableInputFormatCls(job.getConfiguration(), scans));
    if (outputValueClass != null) {
      job.setMapOutputValueClass(outputValueClass);
    }
    if (outputKeyClass != null) {
      job.setMapOutputKeyClass(outputKeyClass);
    }
    job.setMapperClass(mapper);
    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
    List<String> scanStrings = new ArrayList<String>();

    for (Scan scan : scans) {
      scanStrings.add(convertScanToString(scan));
      initCredentials(scan, job);
    }
    job.getConfiguration().setStrings(MultiTableInputFormat.SCANS,
      scanStrings.toArray(new String[scanStrings.size()]));

    if (addDependencyJars) {
      addDependencyJars(job);
    }

    if (initCredentials) {
      initCredentials(job);
    }
  }
  
  private static void initCredentials(Scan scan, Job job) throws IOException {
    byte [] tableNameBytes = scan.getAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME);
    if (tableNameBytes == null) {
      throw new IOException("A scan object did not have a table name");
    }

    String tableName= new String(tableNameBytes);

    Configuration clusterConf = HBaseConfiguration.create(job.getConfiguration());

    if (tableName != null && tableName.startsWith(NameService.HBASE_URI_PREFIX)) {
      ZKUtil.applyClusterKeyToConf(clusterConf, tableName);
    }

    String quorumAddress = job.getConfiguration().get(
            TableOutputFormat.QUORUM_ADDRESS);
    if (quorumAddress != null) {
      ZKUtil.applyClusterKeyToConf(clusterConf, quorumAddress);
    }

    if (User.isHBaseSecurityEnabled(clusterConf)) {
      HConnection conn = HConnectionManager.createConnection(clusterConf);
      try {
        TokenUtil.addTokenForJob(conn, UserProvider.instantiate(job.getConfiguration())
            .getCurrent(), job);
      } catch (InterruptedException e) {
        LOG.info("Interrupted obtaining remote user authentication token");
        Thread.interrupted();
      } finally {
        conn.close();
      }
    }
  }
  
  public static void initCredentials(Job job) throws IOException {
    // for input table name service
    String inputTable = job.getConfiguration().get(TableInputFormat.INPUT_TABLE);
    if (inputTable!= null && inputTable.startsWith(NameService.HBASE_URI_PREFIX)) {
      ZKUtil.applyClusterKeyToConf(job.getConfiguration(), inputTable);
    }

    if (User.isHBaseSecurityEnabled(job.getConfiguration())) {
      HConnection conn = HConnectionManager.createConnection(job.getConfiguration());
      try {
        TokenUtil.addTokenForJob(conn, UserProvider.instantiate(job.getConfiguration())
            .getCurrent(), job);
      } catch (InterruptedException e) {
        LOG.info("Interrupted obtaining user authentication token");
        Thread.interrupted();
      } finally {
        conn.close();
      }
    }

    UserProvider userProvider = UserProvider.instantiate(job.getConfiguration());
    if (userProvider.isHadoopSecurityEnabled()) {
      // propagate delegation related props from launcher job to MR job
      if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
        job.getConfiguration().set("mapreduce.job.credentials.binary",
                                   System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
      }
    }
    
    // for output table name service
    // init credentials for remote cluster
    Configuration peerConf = HBaseConfiguration.create(job
      .getConfiguration());

    String outputTable = job.getConfiguration().get(
      TableOutputFormat.OUTPUT_TABLE);
    if (outputTable != null && outputTable.startsWith(NameService.HBASE_URI_PREFIX)) {
      ZKUtil.applyClusterKeyToConf(peerConf, outputTable);
    }

    String quorumAddress = job.getConfiguration().get(
      TableOutputFormat.QUORUM_ADDRESS);
    if (quorumAddress != null) {
      ZKUtil.applyClusterKeyToConf(peerConf, quorumAddress);
    }

    if (User.isHBaseSecurityEnabled(peerConf)) {
      HConnection peerConn = HConnectionManager.createConnection(peerConf);
      
      try {
        TokenUtil.addTokenForJob(peerConn, UserProvider.instantiate(peerConf)
            .getCurrent(), job);
      } catch (InterruptedException e) {
        LOG.info("Interrupted obtaining remote user authentication token");
        Thread.interrupted();
      } finally {
        peerConn.close();
      }      
    }
  }

  /**
   * Obtain an authentication token, for the specified cluster, on behalf of the current user
   * and add it to the credentials for the given map reduce job.
   *
   * The quorumAddress is the key to the ZK ensemble, which contains:
   * hbase.zookeeper.quorum, hbase.zookeeper.client.port and zookeeper.znode.parent
   *
   * @param job The job that requires the permission.
   * @param quorumAddress string that contains the 3 required configuratins
   * @throws IOException When the authentication token cannot be obtained.
   */
  public static void initCredentialsForCluster(Job job, String quorumAddress)
      throws IOException {
    UserProvider userProvider = UserProvider.instantiate(job.getConfiguration());
    if (userProvider.isHBaseSecurityEnabled()) {
      try {
        Configuration peerConf = HBaseConfiguration.create(job.getConfiguration());
        ZKUtil.applyClusterKeyToConf(peerConf, quorumAddress);
        HConnection peerConn = HConnectionManager.createConnection(peerConf);
        try {
          TokenUtil.addTokenForJob(peerConn, userProvider.getCurrent(), job);
        } finally {
          peerConn.close();
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupted obtaining user authentication token");
        Thread.interrupted();
      }
    }
  }

  /**
   * Writes the given scan into a Base64 encoded string.
   *
   * @param scan  The scan to write out.
   * @return The scan saved in a Base64 encoded string.
   * @throws IOException When writing the scan fails.
   */
  static String convertScanToString(Scan scan) throws IOException {
    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
    return Base64.encodeBytes(proto.toByteArray());
  }

  /**
   * Converts the given Base64 string back into a Scan instance.
   *
   * @param base64  The scan details.
   * @return The newly created Scan instance.
   * @throws IOException When reading the scan instance fails.
   */
  static Scan convertStringToScan(String base64) throws IOException {
    byte [] decoded = Base64.decode(base64);
    ClientProtos.Scan scan;
    try {
      scan = ClientProtos.Scan.parseFrom(decoded);
    } catch (InvalidProtocolBufferException ipbe) {
      throw new IOException(ipbe);
    }

    return ProtobufUtil.toScan(scan);
  }

  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   *
   * @param table  The output table.
   * @param reducer  The reducer class to use.
   * @param job  The current job to adjust.
   * @throws IOException When determining the region count fails.
   */
  public static void initTableReducerJob(String table,
    Class<? extends TableReducer> reducer, Job job)
  throws IOException {
    initTableReducerJob(table, reducer, job, null);
  }

  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   *
   * @param table  The output table.
   * @param reducer  The reducer class to use.
   * @param job  The current job to adjust.
   * @param partitioner  Partitioner to use. Pass <code>null</code> to use
   * default partitioner.
   * @throws IOException When determining the region count fails.
   */
  public static void initTableReducerJob(String table,
    Class<? extends TableReducer> reducer, Job job,
    Class partitioner) throws IOException {
    initTableReducerJob(table, reducer, job, partitioner, null, null, null);
  }

  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   *
   * @param table  The output table.
   * @param reducer  The reducer class to use.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @param partitioner  Partitioner to use. Pass <code>null</code> to use
   * default partitioner.
   * @param quorumAddress Distant cluster to write to; default is null for
   * output to the cluster that is designated in <code>hbase-site.xml</code>.
   * Set this String to the zookeeper ensemble of an alternate remote cluster
   * when you would have the reduce write a cluster that is other than the
   * default; e.g. copying tables between clusters, the source would be
   * designated by <code>hbase-site.xml</code> and this param would have the
   * ensemble address of the remote cluster.  The format to pass is particular.
   * Pass <code> &lt;hbase.zookeeper.quorum>:&lt;hbase.zookeeper.client.port>:&lt;zookeeper.znode.parent>
   * </code> such as <code>server,server2,server3:2181:/hbase</code>.
   * @param serverClass redefined hbase.regionserver.class
   * @param serverImpl redefined hbase.regionserver.impl
   * @throws IOException When determining the region count fails.
   */
  public static void initTableReducerJob(String table,
    Class<? extends TableReducer> reducer, Job job,
    Class partitioner, String quorumAddress, String serverClass,
    String serverImpl) throws IOException {
    initTableReducerJob(table, reducer, job, partitioner, quorumAddress,
        serverClass, serverImpl, true);
  }

  /**
   * Use this before submitting a TableReduce job. It will
   * appropriately set up the JobConf.
   *
   * @param table  The output table.
   * @param reducer  The reducer class to use.
   * @param job  The current job to adjust.  Make sure the passed job is
   * carrying all necessary HBase configuration.
   * @param partitioner  Partitioner to use. Pass <code>null</code> to use
   * default partitioner.
   * @param quorumAddress Distant cluster to write to; default is null for
   * output to the cluster that is designated in <code>hbase-site.xml</code>.
   * Set this String to the zookeeper ensemble of an alternate remote cluster
   * when you would have the reduce write a cluster that is other than the
   * default; e.g. copying tables between clusters, the source would be
   * designated by <code>hbase-site.xml</code> and this param would have the
   * ensemble address of the remote cluster.  The format to pass is particular.
   * Pass <code> &lt;hbase.zookeeper.quorum>:&lt;hbase.zookeeper.client.port>:&lt;zookeeper.znode.parent>
   * </code> such as <code>server,server2,server3:2181:/hbase</code>.
   * @param serverClass redefined hbase.regionserver.class
   * @param serverImpl redefined hbase.regionserver.impl
   * @param addDependencyJars upload HBase jars and jars for any of the configured
   *           job classes via the distributed cache (tmpjars).
   * @throws IOException When determining the region count fails.
   */
  public static void initTableReducerJob(String table,
    Class<? extends TableReducer> reducer, Job job,
    Class partitioner, String quorumAddress, String serverClass,
    String serverImpl, boolean addDependencyJars) throws IOException {

    Configuration conf = job.getConfiguration();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
    job.setOutputFormatClass(TableOutputFormat.class);
    if (reducer != null) job.setReducerClass(reducer);
    conf.set(TableOutputFormat.OUTPUT_TABLE, table);
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        MutationSerialization.class.getName(), ResultSerialization.class.getName());
    // If passed a quorum/ensemble address, pass it on to TableOutputFormat.
    if (quorumAddress != null) {
      // Calling this will validate the format
      ZKUtil.transformClusterKey(quorumAddress);
      conf.set(TableOutputFormat.QUORUM_ADDRESS,quorumAddress);
    }
    if (serverClass != null && serverImpl != null) {
      conf.set(TableOutputFormat.REGION_SERVER_CLASS, serverClass);
      conf.set(TableOutputFormat.REGION_SERVER_IMPL, serverImpl);
    }
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);
    if (partitioner == HRegionPartitioner.class) {
      job.setPartitionerClass(HRegionPartitioner.class);
      int regions = MetaReader.getRegionCount(conf, table);
      if (job.getNumReduceTasks() > regions) {
        job.setNumReduceTasks(regions);
      }
    } else if (partitioner != null) {
      job.setPartitionerClass(partitioner);
    }

    if (addDependencyJars) {
      addDependencyJars(job);
    }

    initCredentials(job);
  }

  /**
   * Ensures that the given number of reduce tasks for the given job
   * configuration does not exceed the number of regions for the given table.
   *
   * @param table  The table to get the region count for.
   * @param job  The current job to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void limitNumReduceTasks(String table, Job job)
  throws IOException {
    int regions = MetaReader.getRegionCount(job.getConfiguration(), table);
    if (job.getNumReduceTasks() > regions)
      job.setNumReduceTasks(regions);
  }

  /**
   * Sets the number of reduce tasks for the given job configuration to the
   * number of regions the given table has.
   *
   * @param table  The table to get the region count for.
   * @param job  The current job to adjust.
   * @throws IOException When retrieving the table details fails.
   */
  public static void setNumReduceTasks(String table, Job job)
  throws IOException {
    job.setNumReduceTasks(MetaReader.getRegionCount(job.getConfiguration(), table));
  }

  /**
   * Sets the number of rows to return and cache with each scanner iteration.
   * Higher caching values will enable faster mapreduce jobs at the expense of
   * requiring more heap to contain the cached rows.
   *
   * @param job The current job to adjust.
   * @param batchSize The number of rows to return in batch with each scanner
   * iteration.
   */
  public static void setScannerCaching(Job job, int batchSize) {
    job.getConfiguration().setInt("hbase.client.scanner.caching", batchSize);
  }

  /**
   * Add HBase and its dependencies (only) to the job configuration.
   * <p>
   * This is intended as a low-level API, facilitating code reuse between this
   * class and its mapred counterpart. It also of use to extenral tools that
   * need to build a MapReduce job that interacts with HBase but want
   * fine-grained control over the jars shipped to the cluster.
   * </p>
   * @param conf The Configuration object to extend with dependencies.
   * @see org.apache.hadoop.hbase.mapred.TableMapReduceUtil
   * @see <a href="https://issues.apache.org/jira/browse/PIG-3285">PIG-3285</a>
   */
  public static void addHBaseDependencyJars(Configuration conf) throws IOException {
    addDependencyJars(conf,
      // explicitly pull a class from each module
      org.apache.hadoop.hbase.HConstants.class,                      // hbase-common
      org.apache.hadoop.hbase.protobuf.generated.ClientProtos.class, // hbase-protocol
      org.apache.hadoop.hbase.client.Put.class,                      // hbase-client
      org.apache.hadoop.hbase.CompatibilityFactory.class,            // hbase-hadoop-compat
      org.apache.hadoop.hbase.mapreduce.TableMapper.class,           // hbase-server
      // pull necessary dependencies
      org.apache.zookeeper.ZooKeeper.class,
      org.jboss.netty.channel.ChannelFactory.class,
      com.google.protobuf.Message.class,
      com.google.common.collect.Lists.class,
      org.apache.htrace.Trace.class,
      org.cliffc.high_scale_lib.Counter.class,
      com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBuf.class); // mapred over snapshots
  }

  /**
   * Returns a classpath string built from the content of the "tmpjars" value in {@code conf}.
   * Also exposed to shell scripts via `bin/hbase mapredcp`.
   */
  public static String buildDependencyClasspath(Configuration conf) {
    if (conf == null) {
      throw new IllegalArgumentException("Must provide a configuration object.");
    }
    Set<String> paths = new HashSet<String>(conf.getStringCollection("tmpjars"));
    if (paths.size() == 0) {
      throw new IllegalArgumentException("Configuration contains no tmpjars.");
    }
    StringBuilder sb = new StringBuilder();
    for (String s : paths) {
      // entries can take the form 'file:/path/to/file.jar'.
      int idx = s.indexOf(":");
      if (idx != -1) s = s.substring(idx + 1);
      if (sb.length() > 0) sb.append(File.pathSeparator);
      sb.append(s);
    }
    return sb.toString();
  }

  /**
   * Add the HBase dependency jars as well as jars for any of the configured
   * job classes to the job configuration, so that JobClient will ship them
   * to the cluster and add them to the DistributedCache.
   */
  public static void addDependencyJars(Job job) throws IOException {
    addHBaseDependencyJars(job.getConfiguration());
    try {
      addDependencyJars(job.getConfiguration(),
          // when making changes here, consider also mapred.TableMapReduceUtil
          // pull job classes
          job.getMapOutputKeyClass(),
          job.getMapOutputValueClass(),
          job.getInputFormatClass(),
          job.getOutputKeyClass(),
          job.getOutputValueClass(),
          job.getOutputFormatClass(),
          job.getPartitionerClass(),
          job.getCombinerClass());
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Add the jars containing the given classes to the job's configuration
   * such that JobClient will ship them to the cluster and add them to
   * the DistributedCache.
   */
  public static void addDependencyJars(Configuration conf,
      Class<?>... classes) throws IOException {

    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<String>();
    // Add jars that are already in the tmpjars variable
    jars.addAll(conf.getStringCollection("tmpjars"));

    // add jars as we find them to a map of contents jar name so that we can avoid
    // creating new jars for classes that have already been packaged.
    Map<String, String> packagedClasses = new HashMap<String, String>();

    // Add jars containing the specified classes
    for (Class<?> clazz : classes) {
      if (clazz == null) continue;

      Path path = findOrCreateJar(clazz, localFs, packagedClasses);
      if (path == null) {
        LOG.warn("Could not find jar for class " + clazz +
                 " in order to ship it to the cluster.");
        continue;
      }
      if (!localFs.exists(path)) {
        LOG.warn("Could not validate jar file " + path + " for class "
                 + clazz);
        continue;
      }
      jars.add(path.toString());
    }
    if (jars.isEmpty()) return;

    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
  }

  /**
   * If org.apache.hadoop.util.JarFinder is available (0.23+ hadoop), finds
   * the Jar for a class or creates it if it doesn't exist. If the class is in
   * a directory in the classpath, it creates a Jar on the fly with the
   * contents of the directory and returns the path to that Jar. If a Jar is
   * created, it is created in the system temporary directory. Otherwise,
   * returns an existing jar that contains a class of the same name. Maintains
   * a mapping from jar contents to the tmp jar created.
   * @param my_class the class to find.
   * @param fs the FileSystem with which to qualify the returned path.
   * @param packagedClasses a map of class name to path.
   * @return a jar file that contains the class.
   * @throws IOException
   */
  private static Path findOrCreateJar(Class<?> my_class, FileSystem fs,
      Map<String, String> packagedClasses)
  throws IOException {
    // attempt to locate an existing jar for the class.
    String jar = findContainingJar(my_class, packagedClasses);
    if (null == jar || jar.isEmpty()) {
      jar = getJar(my_class);
      if (jar != null) {
        updateMap(jar, packagedClasses);
      }
    }

    if (null == jar || jar.isEmpty()) {
      return null;
    }

    LOG.debug(String.format("For class %s, using jar %s", my_class.getName(), jar));
    return new Path(jar).makeQualified(fs);
  }

  /**
   * Add entries to <code>packagedClasses</code> corresponding to class files
   * contained in <code>jar</code>.
   * @param jar The jar who's content to list.
   * @param packagedClasses map[class -> jar]
   */
  private static void updateMap(String jar, Map<String, String> packagedClasses) throws IOException {
    if (null == jar || jar.isEmpty()) {
      return;
    }
    ZipFile zip = null;
    try {
      zip = new ZipFile(jar);
      for (Enumeration<? extends ZipEntry> iter = zip.entries(); iter.hasMoreElements();) {
        ZipEntry entry = iter.nextElement();
        if (entry.getName().endsWith("class")) {
          packagedClasses.put(entry.getName(), jar);
        }
      }
    } finally {
      if (null != zip) zip.close();
    }
  }

  /**
   * Find a jar that contains a class of the same name, if any. It will return
   * a jar file, even if that is not the first thing on the class path that
   * has a class with the same name. Looks first on the classpath and then in
   * the <code>packagedClasses</code> map.
   * @param my_class the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  private static String findContainingJar(Class<?> my_class, Map<String, String> packagedClasses)
      throws IOException {
    ClassLoader loader = my_class.getClassLoader();

    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    if (loader == null) return null;
    if (loader != null) {
      // first search the classpath
      for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements();) {
        URL url = itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          // URLDecoder is a misnamed class, since it actually decodes
          // x-www-form-urlencoded MIME type rather than actual
          // URL encoding (which the file path has). Therefore it would
          // decode +s to ' 's which is incorrect (spaces are actually
          // either unencoded or encoded as "%20"). Replace +s first, so
          // that they are kept sacred during the decoding process.
          toReturn = toReturn.replaceAll("\\+", "%2B");
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    }

    // now look in any jars we've packaged using JarFinder. Returns null when
    // no jar is found.
    return packagedClasses.get(class_file);
  }

  /**
   * Invoke 'getJar' on a JarFinder implementation. Useful for some job
   * configuration contexts (HBASE-8140) and also for testing on MRv2. First
   * check if we have HADOOP-9426. Lacking that, fall back to the backport.
   * @param my_class the class to find.
   * @return a jar file that contains the class, or null.
   */
  private static String getJar(Class<?> my_class) {
    String ret = null;
    String hadoopJarFinder = "org.apache.hadoop.util.JarFinder";
    Class<?> jarFinder = null;
    try {
      LOG.debug("Looking for " + hadoopJarFinder + ".");
      jarFinder = Class.forName(hadoopJarFinder);
      LOG.debug(hadoopJarFinder + " found.");
      Method getJar = jarFinder.getMethod("getJar", Class.class);
      ret = (String) getJar.invoke(null, my_class);
    } catch (ClassNotFoundException e) {
      LOG.debug("Using backported JarFinder.");
      ret = JarFinder.getJar(my_class);
    } catch (InvocationTargetException e) {
      // function was properly called, but threw it's own exception. Unwrap it
      // and pass it on.
      throw new RuntimeException(e.getCause());
    } catch (Exception e) {
      // toss all other exceptions, related to reflection failure
      throw new RuntimeException("getJar invocation failed.", e);
    }

    return ret;
  }
  
  /**
   * Control the scan rate
   * @param scanRateLimit the scan rate limit
   * @param rowdone  number of rows have been done
   * @param timeUsed the time costed
   */
  public static void limitScanRate(int scanRateLimit, long rowdone, long timeUsed) {
    if (scanRateLimit > 0) {
      long sleepTimeInMs = 1000 * rowdone / scanRateLimit - timeUsed;
      if (sleepTimeInMs > 0) {
        try {
          Thread.sleep(sleepTimeInMs);
        } catch (InterruptedException e) {
          // do nothing.
        }
      }
    }
  }
}

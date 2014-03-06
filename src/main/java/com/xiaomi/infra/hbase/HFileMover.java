/**
 * Copyright 2014, 
 * Xiaomi.com All rights reserved. 
 * Author: liushaohui
 */

package com.xiaomi.infra.hbase;

import java.io.IOException;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class HFileMover extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(HFileMover.class);
  private String tableName;
  private Path dstPath;

  /** default constructor */
  public HFileMover() {
    super();
  }

  /**
   * @param conf configuration
   */
  public HFileMover(Configuration conf) {
    setConf(conf);
  }

  private void usage() {
    System.err.println("Usage: bin/hbase com.xiaomi.infra.hbase.HFileMover "
        + "<table-name> <dst-path>\n");
  }

  /**
   * Represents an HFile waiting to be moved.
   */
  static class MoveItem {
    final String family;
    final Path hfilePath;

    public MoveItem(String family, Path hfilePath) {
      this.family = family;
      this.hfilePath = hfilePath;
    }

    public String toString() {
      return "family:" + family + " path:" + hfilePath.toString();
    }
  }

  private int parseArgs(String[] args) throws IOException {
    GenericOptionsParser parser = new GenericOptionsParser(getConf(), args);

    String[] remainingArgs = parser.getRemainingArgs();
    if (remainingArgs.length != 2) {
      usage();
      return -1;
    }

    tableName = remainingArgs[0];
    dstPath = new Path(remainingArgs[1]);
    return 0;
  }

  public int run(String[] args) throws Exception {
    if (parseArgs(args) != 0) {
      return -1;
    }

    // Verify file system is up.
    final FileSystem fs = FileSystem.get(getConf()); // get DFS handle
    LOG.info("Verifying that file system is available...");
    try {
      FSUtils.checkFileSystemAvailable(fs);
    } catch (IOException e) {
      LOG.fatal("File system is not available", e);
      return -1;
    }

    // Verify table is disabled
    LOG.info("Verifying that HBase is not running...");
    HBaseAdmin admin = new HBaseAdmin(getConf());
    if (!admin.isTableDisabled(tableName)) {
      LOG.fatal("Table must be disabled.");
      return -1;
    }

    Path tablePath = new Path(FSUtils.getRootDir(getConf()), tableName);
    Deque<MoveItem> queue = new LinkedList<MoveItem>();
    
    HTableDescriptor desc = admin.getTableDescriptor(Bytes.toBytes(tableName));
    
    HTable table = new HTable(getConf(), tableName);
    for (HRegionInfo info : table.getRegionLocations().keySet()) {
      Path regionPath = new Path(tablePath, info.getEncodedName());
      for (HColumnDescriptor familyDesc : desc.getColumnFamilies()) {
        String familyName = familyDesc.getNameAsString();
        Path familyPath = new Path(regionPath, familyName);
        for (FileStatus hfile : fs.listStatus(familyPath)) {
          if (StoreFile.isReference(hfile.getPath())) {
            LOG.fatal("Hfile is reference. Path: " + hfile.getPath());
            return -1;
          }
          queue.add(new MoveItem(familyName, hfile.getPath()));
        }
      }
    }

    // create dst table and store dir
    if (!fs.exists(dstPath)) {
      fs.mkdirs(dstPath);
    }
    Path dstTablePath = new Path(dstPath, tableName);
    if (!fs.exists(dstTablePath)) {
      fs.mkdirs(dstTablePath);
    }
    for (HColumnDescriptor familyDesc : desc.getColumnFamilies()) {
      String familyName = familyDesc.getNameAsString();
      Path dstFamilyPath = new Path(dstTablePath, familyName);
      if (!fs.exists(dstFamilyPath)) {
        fs.mkdirs(dstFamilyPath);
      }
    }

    // do moving hfile in multi threads
    int nrThreads =
        getConf().getInt("hbase.hfilemover.threads.max",
          Runtime.getRuntime().availableProcessors());
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("hfilemover-%1$d");
    ExecutorService pool =
        new ThreadPoolExecutor(nrThreads, nrThreads, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(), builder.build());

    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
    Set<Future<Void>> movingFutures = new HashSet<Future<Void>>();
    for (final MoveItem item : queue) {
      final Path hfile = item.hfilePath;
      final Path dstFamilyPath = new Path(dstTablePath, item.family);
      final Callable<Void> call = new Callable<Void>() {
        public Void call() throws Exception {
          LOG.info("move hfile: " + hfile + " to path: " + dstFamilyPath);
          fs.rename(hfile, dstFamilyPath);
          return null;
        }
      };
      movingFutures.add(pool.submit(call));
    }

    // get all the results.
    for (Future<Void> future : movingFutures) {
      future.get();
    }

    table.close();
    admin.close();
    return 0;
  }

  public static void main(String[] args) {
    int status;
    try {
      status =
          ToolRunner.run(HBaseConfiguration.create(), new HFileMover(), args);
    } catch (Exception e) {
      LOG.error("exiting due to error", e);
      status = -1;
    }
    System.exit(status);
  }
}

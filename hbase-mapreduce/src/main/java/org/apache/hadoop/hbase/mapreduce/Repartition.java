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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.mapreduce.ImportSnapshot.BULK_OUTPUT_CONF_KEY;
import static org.apache.hadoop.hbase.mapreduce.ImportSnapshot.SNAPSHOTSCANNER_SPLIT_NUM;
import static org.apache.hadoop.hbase.mapreduce.ImportSnapshot.SNAPSHOT_RESTORE_PATH;
import static org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles.CREATE_TABLE_CONF_KEY;

/**
 * 1. disable table
 * 2. create a snapshot
 * 3. drop table
 * 4. create new table based on new split file
 * 5. bulk load snapshot files to new table
 */
@InterfaceAudience.Public
public class Repartition {
  private static final Logger LOG = LoggerFactory.getLogger(Repartition.class);

  private TableName tableName;
  private Admin admin;
  private String splitFile;
  private int numRegion = -1;
  private String splitAlgo;
  private int slots = -1;
  private String snapshotName;
  private Configuration conf;
  private TableDescriptor htd = null;
  private FileSystem fs;

  public enum State {
    PREPARE_SUCC, DISABLE_SUCC, SNAPSHOT_SUCC, DROP_SUCC, CREATE_SUCC, BULK_LOAD_SUCC, IMPORT_SUCC,
    PROCESS_SUCC, PROCESS_FAILED, REPAIR_AFTER_FAILURE
  }

  public Repartition(String[] args) throws IOException {
    this(HBaseConfiguration.create(), args);
  }

  public Repartition(Configuration conf, String[] args) throws IOException {
    this.conf = parseCommandLine(args, conf);
    checkConf();
    LOG.info("start to repartition table " + tableName + "; outputPath"
        + conf.get(BULK_OUTPUT_CONF_KEY) + "; restorePath" + conf.get(SNAPSHOT_RESTORE_PATH));
    try {
      this.fs = FileSystem.get(URI.create(conf.get(HConstants.HBASE_DIR)), conf);
      if (fs.exists(new Path(conf.get(BULK_OUTPUT_CONF_KEY)))) {
        throw new RuntimeException("--export-to path should not exist, please delete the dir");
      }
    } catch (IOException e) {
      LOG.error("configuration should have a valid, please check");
      printUsageAndExit();
    }
    init();
  }

  private void init() {
    try {
      Connection connection = ConnectionFactory.createConnection(conf);
      admin = connection.getAdmin();
      htd = admin.getDescriptor(tableName);
    } catch (IOException e) {
      LOG.error("get table admin or descriptor failed", e);
    }
  }

  public void setAdmin(Admin admin) {
    this.admin = admin;
  }

  private void checkConf() {
    if (tableName == null || conf.get(BULK_OUTPUT_CONF_KEY) == null
        || conf.get(SNAPSHOT_RESTORE_PATH) == null) {
      LOG.error("some required parameter is not set, please check!!!!");
      printUsageAndExit();
    }
    if (splitAlgo == null && splitFile == null && slots == -1) {
      System.err.println("splitting way must be set, choose one");
      printUsageAndExit();
    }

    if (splitAlgo != null) {
      if (numRegion == -1) {
        System.err.println("numRegion must be set if you set splitAlgo");
        printUsageAndExit();
      }
    }
  }

  public boolean repatitionProcess() {
    State state = State.PREPARE_SUCC;
    State prevState = state;
    boolean stepSucc;
    while (true) {
      switch (state) {
      case PREPARE_SUCC:
        prevState = state;
        stepSucc = disableTable();
        if (stepSucc) {
          state = State.DISABLE_SUCC;
          LOG.info("Repartition: disable successful");
        } else {
          state = State.PROCESS_FAILED;
        }
        break;
      case DISABLE_SUCC:
        prevState = state;
        stepSucc = snapshot();
        if (stepSucc) {
          state = State.SNAPSHOT_SUCC;
          LOG.info("Repartition: snapshot successful");
        } else {
          state = State.PROCESS_FAILED;
        }
        break;
      case SNAPSHOT_SUCC:
        prevState = state;
        stepSucc = dropTable();
        if (stepSucc) {
          state = State.DROP_SUCC;
          LOG.info("Repartition: drop successful");
        } else {
          state = State.PROCESS_FAILED;
        }
        break;
      case DROP_SUCC:
        prevState = state;
        stepSucc = createTable();
        if (stepSucc) {
          state = State.CREATE_SUCC;
          LOG.info("Repartition: create new table successful");
        } else {
          state = State.REPAIR_AFTER_FAILURE;
        }
        break;
      case CREATE_SUCC:
        prevState = state;
        stepSucc = importSnapshot();
        if (stepSucc) {
          state = State.IMPORT_SUCC;
          LOG.info("Repartition: import snapshot successful");
        } else {
          state = State.REPAIR_AFTER_FAILURE;
        }
        break;
      case IMPORT_SUCC:
        prevState = state;
        stepSucc = bulkloadSnapshot();
        if (stepSucc) {
          state = State.BULK_LOAD_SUCC;
          LOG.info("Repartition: bulk load snapshot successful");
        } else {
          state = State.REPAIR_AFTER_FAILURE;
        }
        break;
      case BULK_LOAD_SUCC:
        LOG.info("repartition process success, exiting now...");
        cleanup();
        return true;
      case REPAIR_AFTER_FAILURE:
        prevState = state;
        stepSucc = restoreSnapshot();
        if (stepSucc) {
          LOG.info("table repaired successful");
        } else {
          LOG.info(
              "repaired failed after state " + prevState.name() + ", please repair table manually");
        }
        return false;
      case PROCESS_FAILED:
        LOG.error("process failed at " + prevState.name() + ", please retry");
        cleanup();
        return false;
      }
    }
  }

  private void cleanup() {
    try {
      if (admin != null) {
        admin.close();
      }
      if (fs != null) {
        fs.close();
      }
    } catch (IOException e) {
      LOG.error("admin close failed, ", e);
    }
  }

  private boolean restoreSnapshot() {
    try {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      admin.restoreSnapshot(snapshotName);
    } catch (IOException e) {
      LOG.error("restore failed, please do it manually");
      return false;
    }
    return true;
  }

  private boolean bulkloadSnapshot() {
    String outputPath = conf.get(BULK_OUTPUT_CONF_KEY);
    conf.setBoolean(CREATE_TABLE_CONF_KEY, false);
    int code = 0;
    try {
      code = new LoadIncrementalHFiles(conf)
          .run(new String[] { outputPath, tableName.getNameAsString() });
      if (code == 0) {
        // bulkloadDir is deleted only LoadIncrementalHFiles was successful so that one can rerun
        // LoadIncrementalHFiles.
        FileSystem fs = FileSystem.get(new URI(outputPath), conf);
        if (!fs.delete(new Path(outputPath), true)) {
          LOG.error("Deleting folder " + outputPath + " failed!");
          code = 1;
        }
      }
    } catch (Exception e) {
      LOG.error("bulk load snapshot failed", e);
      return false;
    }
    if (code == 1) {
      LOG.error("folder deleting failed, please delete it manually");
      return false;
    }
    return true;
  }

  private boolean importSnapshot() {
    if (snapshotName == null) {
      return false;
    }
    String[] args = new String[] { tableName.getNameAsString(), snapshotName };
    try {
      Job job = ImportSnapshot.createSubmittableJob(conf, args);
      return job.waitForCompletion(true);
    } catch (IOException | ClassNotFoundException | InterruptedException e) {
      LOG.error("ImportSnapshot job failed", e);
      return false;
    }
  }

  private boolean disableTable() {
    try {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
    } catch (IOException e) {
      LOG.error("disable table failed", e);
      return false;
    }
    return true;
  }

  private boolean snapshot() {
    long current = System.currentTimeMillis();
    snapshotName = tableName.getNamespaceAsString() + "" + tableName.getQualifierAsString()
        + String.valueOf(current);
    try {
      admin.snapshot(snapshotName, tableName);
    } catch (IOException e) {
      LOG.error("snapshot table failed", e);
      return false;
    }
    return true;
  }

  private boolean dropTable() {
    try {
      admin.deleteTable(tableName);
    } catch (IOException e) {
      LOG.error("create hbase admin failed", e);
      return false;
    }
    return true;
  }

  private boolean createTable() {
    // get the split keys from file
    byte[][] keys = null;
    TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(htd);
    try {
      if (splitFile != null) {
        String str;
        List<byte[]> keyList = new ArrayList<>();
        try (FileInputStream inputStream = new FileInputStream(splitFile);
            BufferedReader bufferedReader =
                new BufferedReader(new InputStreamReader(inputStream))) {
          while ((str = bufferedReader.readLine()) != null) {
            keyList.add(Bytes.toBytesBinary(str));
          }
          keys = keyList.toArray(new byte[0][]);
        }
      } else if (splitAlgo != null) {
        RegionSplitter.SplitAlgorithm splitAlgorithm =
            RegionSplitter.newSplitAlgoInstance(conf, splitAlgo);
        keys = splitAlgorithm.split(numRegion);
      } else {
        tdBuilder.setSlotsCount(slots);
      }

      admin.createTable(tdBuilder.build(), keys);

    } catch (IOException e) {
      LOG.error("create table failed", e);
      return false;
    }
    return true;
  }

  private void printUsageAndExit() {
    System.err.println("Usage: ./hbase " + getClass().getName() + " \\");
    System.err.println("        --table <tableName>");
    System.err.println("        --export-to <outputPath>");
    System.err.println("        --restorePath <restorePath>");
    System.err.println(
        "        --splitFile <splitFile> | --slots <slots> | --splitAlgo <splitAlgo> --numRegion <numRegion>");
    System.err.println("        --snapshotSplitNum [snapshotSplitNum]");
    System.err.println();
    System.err.println("Examples:");
    System.err.println("  hbase " + getClass().getName() + " \\");
    System.err.println("    --table MyTable --export-to hdfs://srv2:8082/hbase \\");
    System.err.println("    --restorePath hdfs://srv2:8082/restore");
    System.err.println("    --slots 256");
    System.err.println("    --snapshotSplitNum 100");
    System.err.println();
    System.exit(1);
  }

  private Configuration parseCommandLine(String[] args, Configuration conf) {
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equalsIgnoreCase("--table")) {
        this.tableName = TableName.valueOf(args[++i]);
      } else if (cmd.equalsIgnoreCase("--export-to")) {
        String outputPath = args[++i];
        conf.set(BULK_OUTPUT_CONF_KEY, outputPath);
      } else if (cmd.equalsIgnoreCase("--restorePath")) {
        String restorePath = args[++i];
        conf.set(SNAPSHOT_RESTORE_PATH, restorePath);
      } else if (cmd.equalsIgnoreCase("--splitFile")) {
        this.splitFile = args[++i];
      } else if (cmd.equalsIgnoreCase("--numRegion")) {
        this.numRegion = Integer.parseInt(args[++i]);
      } else if (cmd.equalsIgnoreCase("--slots")) {
        this.slots = Integer.parseInt(args[++i]);
      } else if (cmd.equalsIgnoreCase("--splitAlgo")) {
        this.splitAlgo = args[++i];
      } else if (cmd.equalsIgnoreCase("--snapshotSplitNum")) {
        conf.set(SNAPSHOTSCANNER_SPLIT_NUM, args[++i]);
      } else {
        System.err.println("UNEXPECTED: " + cmd);
        printUsageAndExit();
      }
    }
    return conf;
  }

  public static void main(String[] args) {
    try {
      Repartition repartitionBySplitFile = new Repartition(args);
      boolean isSucc = repartitionBySplitFile.repatitionProcess();
      if (isSucc) {
        LOG.info("repartition success");
      } else {
        LOG.error("repartition failed, please check reason");
      }
    } catch (IOException e) {
      LOG.error("config failed, please check", e);
    }

  }
}

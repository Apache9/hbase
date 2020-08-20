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
package org.apache.hadoop.hbase.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStateStore;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.snapshot.CreateSnapshot;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This is a command line class that will assign regions from meta (FAILED_CLOSE, PENDING_CLOSE,
 * FAILED_OPEN, PENDING_OPEN, CLOSING) or specified by commandline.
 */
public class RegionAssigner extends AbstractHBaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(RegionAssigner.class);
  private static final Set<RegionState.State> targetRegionStates = ImmutableSet.of(
      RegionState.State.FAILED_CLOSE,
      RegionState.State.PENDING_CLOSE,
      RegionState.State.FAILED_OPEN,
      RegionState.State.PENDING_OPEN,
      RegionState.State.CLOSING);

  private static final String ACTION_LIST = "list";
  private static final String ACTION_ASSIGN = "assign";
  private static final String ACTION_RUN = "run";
  private static final String SPECIFY_FILE = "file";
  private static final String SPECIFY_NAME = "name";
  private static final String BATCH = "batch";

  private int batch = 100;
  private String encodedNameFile;
  private String encodedNames;
  private boolean isListRegion;
  private boolean isAssignRegion;

  public static void main(String[] args) {
    new RegionAssigner().doStaticMain(args);
  }

  @Override
  protected void addOptions() {
    this.addOptNoArg("l", "list", "List all offline regions");
    this.addOptNoArg("a", "assign",
        "Assign all offline regions from meta(if no regions specified) "
            + "or specified regions by encoded name or file."
            + "(But if specifies from command, we won't load offline regions from meta as we may just wanna test)");
    this.addOptNoArg("r", "run",
        "List and assign regions from meta or specified from command" +
            "(But if specifies from command, we won't load offline regions from meta as we may just wanna test)");
    this.addOptWithArg("n", "name",
        "Specify the target region's encoded names to assign, " +
            "we won't load regions from meta as we may just wanna test");
    this.addOptWithArg("f", "file",
        "Specify the target region's encoded names to assign in file, " +
            "we won't load regions from meta as we may just wanna test");
    this.addOptWithArg("b", "batch",
        "Specify the batch size in one bulkAssign rpc, the default size is 100");
  }

  @VisibleForTesting
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    if (cmd.hasOption(ACTION_LIST) || cmd.hasOption(ACTION_RUN)) {
      isListRegion = true;
    }

    if (cmd.hasOption(ACTION_ASSIGN) || cmd.hasOption(ACTION_RUN)) {
      isAssignRegion = true;
    }
    batch = 100;
    if (cmd.hasOption(BATCH)) {
      batch = Integer.parseInt(cmd.getOptionValue(BATCH));
    }
    encodedNameFile = cmd.getOptionValue(SPECIFY_FILE);
    encodedNames = cmd.getOptionValue(SPECIFY_NAME);
  }

  private Map<HRegionInfo, RegionState.State> getRegionStatesFromFile(String fileName) throws IOException {
    Set<String> encodeNames = new HashSet<>();
    File file = new File(fileName);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line = null;
    while((line = reader.readLine()) != null) {
      LOG.info("read encodedName "  + line + " from "+ fileName);
      List<String> encodedNames = Arrays.stream(line.split(",| "))
          .filter(StringUtils::isNotBlank).map(String::trim).collect(Collectors.toList());
      encodeNames.addAll(encodedNames);
    }
    return getTargetRegionStates(encodeNames);
  }

  private Map<HRegionInfo, RegionState.State> getRegionStatesFromString(String encodedNames) {
    LOG.info("read encodeName from string: " + encodedNames);
    Set<String> encodedNameSet = Arrays.stream(encodedNames.split(",| "))
        .filter(StringUtils::isNotBlank).map(String::trim).collect(Collectors.toSet());
    return getTargetRegionStates(encodedNameSet);
  }

  @VisibleForTesting
  public void execute() throws IOException,InterruptedException {
    doWork();
  }

  @Override
  protected int doWork() throws IOException,InterruptedException {
    Map<HRegionInfo, RegionState.State> regionStates = new TreeMap<>();
    if (encodedNameFile != null) {
      LOG.info("Reading encodedName from file " + encodedNameFile);
      Map<HRegionInfo, RegionState.State> regionStatesFromFile = getRegionStatesFromFile(encodedNameFile);
      regionStates.putAll(regionStatesFromFile);
    }
    if (StringUtils.isNotBlank(encodedNames)) {
      LOG.info("Reading encodedName from commandline " + encodedNames);
      Map<HRegionInfo, RegionState.State> regionStatesFromString = getRegionStatesFromString(encodedNames);
      regionStates.putAll(regionStatesFromString);
    }

    if (regionStates.isEmpty()) {
      regionStates = getOfflineRegionStates();
    } else {
      LOG.info("As you specify regions from command, so won't load offline regions from meta.");
    }
    List<HRegionInfo> assigningRegionInfos = new ArrayList<>();
    for (Map.Entry<HRegionInfo, RegionState.State> entry : regionStates.entrySet()) {
      if (isListRegion) {
        LOG.info("Region to assign: " + entry.getKey().getRegionNameAsString() + " encodedName: "
            + entry.getKey().getEncodedName() + " state: " + entry.getValue());
      }
      assigningRegionInfos.add(entry.getKey());
    }
    if (isAssignRegion) {
      LOG.info("Will assign regions, size = " + assigningRegionInfos.size() + " after 10 s.");
      Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      int ret = assignRegions(assigningRegionInfos);
      return logAndReturn(ret);
    } else {
      return 0;
    }
  }

  private int logAndReturn(int ret) {
    if (ret == 0) {
      LOG.info("Success to assign all offline regions. Return code = " + ret);
    } else if (ret > 0) {
      LOG.error("Partial failed to assign offline regions! Failed bulk count = " + ret);
    } else {
      LOG.error("Failed to execute assigning offline regions! Return code = " + ret);
    }
    return ret;
  }

  public Map<HRegionInfo, RegionState.State> getTargetRegionStates(Set<String> regionEncodedNames) {
    Map<HRegionInfo, RegionState.State> regionStates = new HashMap<>();
    MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitorBase() {
      @Override
      public boolean processRow(Result r) throws IOException {
        if (regionStates.size() >= regionEncodedNames.size()) {
          return false;
        }
        if (r == null || r.isEmpty()) return true;
        HRegionInfo info = HRegionInfo.getHRegionInfo(r);
        if (info == null) return true; // Keep scanning
        if (!regionEncodedNames.contains(info.getEncodedName())) {
          return true;
        }
        regionStates.put(info, RegionStateStore.getRegionState(r));
        return true;
      }
    };
    try {
      MetaScanner.metaScan(conf, null, visitor, null);
    } catch (IOException e) {
      LOG.error("Failed to scan meta ", e);
    }
    return regionStates;
  }

  @VisibleForTesting
  public Map<HRegionInfo, RegionState.State> getOfflineRegionStates() {
    Map<HRegionInfo, RegionState.State> regionStates = new HashMap<>();
    MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitorBase() {
      @Override
      public boolean processRow(Result r) throws IOException {
        if (r == null || r.isEmpty()) return true;
        HRegionInfo info = HRegionInfo.getHRegionInfo(r);
        if (info == null) return true; // Keep scanning
        RegionState.State regionState = RegionStateStore.getRegionState(r);
        LOG.debug("HRegionInfo : " + info + " RegionState = " + regionState
            + ", isSplitParent = " + info.isSplitParent());
        if (info.isSplitParent()
            ||  !targetRegionStates.contains(regionState)) {
          return true;
        }
        regionStates.put(info,
            regionState);
        return true;
      }
    };
    try {
      MetaScanner.metaScan(conf, null, visitor, null);
    } catch (IOException e) {
      LOG.error("Failed to scan meta ", e);
    }
    return regionStates;
  }

  public int assignRegions(List<HRegionInfo> regionInfoList) {
    if (regionInfoList == null || regionInfoList.isEmpty()) {
      LOG.info("No region is in RIT.");
      return 0;
    }
    int ret = 0;
    Set<TableName> tableNames = regionInfoList.stream().map(HRegionInfo::getTable)
        .collect(Collectors.toSet());
    final Set<TableName> enabledTableNameSet = new HashSet<>();
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      enabledTableNameSet.addAll(tableNames.stream().filter(tableName -> {
        try {
          return admin.isTableEnabled(tableName);
        } catch (IOException e) {
          LOG.warn("Exception when get TableName status " + tableName, e);
          return false;
        }
      }).collect(Collectors.toSet()));

      Map<Boolean, List<HRegionInfo>> partitionedRegionsByTableStatus = regionInfoList.stream()
          .collect(Collectors.partitioningBy(regionInfo ->
              enabledTableNameSet.contains(regionInfo.getTable())));
      List<HRegionInfo> invalidTableStatusRegionInfos = partitionedRegionsByTableStatus
          .getOrDefault(false, ImmutableList.of());
      for (HRegionInfo regionInfo : invalidTableStatusRegionInfos) {
        LOG.info("Skip assigning " + regionInfo.getRegionNameAsString()
            + " bacause the table is disabled or unknown.");
      }
      List<List<HRegionInfo>> regionInfoLists = Lists.partition(partitionedRegionsByTableStatus
          .getOrDefault(true, ImmutableList.of()), batch);
      MasterProtos.MasterService.BlockingInterface master = admin.getConnection().getMaster();
      for (List<HRegionInfo> regionInfoSublist : regionInfoLists) {
        String regionNames = regionInfoSublist.stream().map(HRegionInfo::getRegionNameAsString)
            .collect(Collectors.joining(","));
        try {
          master.bulkAssignRegion(null, RequestConverter.buildBulkAssignRegionRequest(regionInfoSublist));
          LOG.info("Success to bulkAssign regions: \n{}", regionNames);
          Thread.sleep(1000);
        } catch (Exception e) {
          LOG.error("Failed to bulkAssign regions: \n{}, \n{}", regionNames, e);
          ++ret;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to build connection for HBaseAdmin {}", e.getLocalizedMessage());
      return -1;
    }
    return ret;
  }
}

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
package org.apache.hadoop.hbase.master.normalizer;

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Simple implementation of region normalizer.
 *
 * Logic in use:
 *
 *  - get all regions of a given table
 *  - get avg size S of each region (by total size of store files reported in RegionLoad)
 *  - If biggest region is bigger than S * 2, it is kindly requested to split,
 *    and normalization stops
 *  - Otherwise, two smallest region R1 and its smallest neighbor R2 are kindly requested
 *    to merge, if R1 + R1 <  S, and normalization stops
 *  - Otherwise, no action is performed
 */
@InterfaceAudience.Private
public class SimpleRegionNormalizer implements RegionNormalizer {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleRegionNormalizer.class);
  private MasterServices masterServices;

  /**
   * Set the master service.
   * @param masterServices inject instance of MasterServices
   */
  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  // Comparator that gives higher priority to region Split plan
  private Comparator<NormalizationPlan> planComparator = (plan, plan2) -> {
    if (plan instanceof SplitNormalizationPlan) {
      return -1;
    }
    if (plan2 instanceof SplitNormalizationPlan) {
      return 1;
    }
    return 0;
  };

  /**
   * Computes next most "urgent" normalization action on the table.
   * Action may be either a split, or a merge, or no action.
   *
   * @param table table to normalize
   * @return normalization plan to execute
   */
  @Override
  public List<NormalizationPlan> computePlanForTable(TableName table) {
    List<NormalizationPlan> plans = new ArrayList<>();
    if (table == null || table.isSystemTable()) {
      LOG.debug("Normalization of table " + table + " isn't allowed");
      return plans;
    }

    List<HRegionInfo> tableRegions = masterServices.getAssignmentManager().getRegionStates().
      getRegionsOfTable(table);

    //TODO: should we make min number of regions a config param?
    if (tableRegions == null || tableRegions.size() < 3) {
      LOG.debug("Table " + table + " has " + tableRegions.size() + " regions, required min number"
        + " of regions for normalizer to run is 3, not running normalizer");
      return plans;
    }

    LOG.debug("Computing normalization plan for table: " + table +
      ", number of regions: " + tableRegions.size());

    long totalSizeMb = 0;
    int acutalRegionCnt = 0;

    for (int i = 0; i < tableRegions.size(); i++) {
      HRegionInfo hri = tableRegions.get(i);
      long regionSize = getRegionSize(hri);
      if (regionSize > 0) {
        acutalRegionCnt++;
        totalSizeMb += regionSize;
      }
    }
    int targetRegionCount = -1;
    long targetRegionSize = -1;
    try {
      HTableDescriptor tableDescriptor = masterServices.getTableDescriptors().get(table);
      if(tableDescriptor != null) {
        targetRegionCount =
            tableDescriptor.getNormalizeTargetRegionCount();
        targetRegionSize =
            tableDescriptor.getNormalizeTargetRegionSize();
        LOG.debug("Table {}:  target region count is {}, target region size is {}",
          Lists.newArrayList(table.getNameAsString(), String.valueOf(targetRegionCount),
            String.valueOf(targetRegionSize)).toArray(new String[0]));
      }
    } catch (IOException e) {
      LOG.warn(
          "cannot get the target number and target size of table {}, they will be default value -1.",
          table);
    }

    double avgRegionSize;
    if (targetRegionSize > 0) {
      avgRegionSize = targetRegionSize;
    } else if (targetRegionCount > 0) {
      avgRegionSize = totalSizeMb / (double) targetRegionCount;
    } else {
      avgRegionSize = acutalRegionCnt == 0 ? 0 : totalSizeMb / (double) acutalRegionCnt;
    }

    LOG.debug("Table " + table + ", total aggregated regions size: " + totalSizeMb);
    LOG.debug("Table " + table + ", average region size: " + avgRegionSize);

    int candidateIdx = 0;
    while (candidateIdx < tableRegions.size()) {
      HRegionInfo hri = tableRegions.get(candidateIdx);
      long regionSize = getRegionSize(hri);
      // if the region is > 2 times larger than average, we split it, split
      // is more high priority normalization action than merge.
      if (regionSize > 2 * avgRegionSize) {
        LOG.info("Table " + table + ", large region " + hri.getRegionNameAsString() + " has size "
            + regionSize + ", more than twice avg size, splitting");
        plans.add(new SplitNormalizationPlan(hri, null));

      } else {
        if (candidateIdx == tableRegions.size() - 1) {
          break;
        }
        HRegionInfo hri2 = tableRegions.get(candidateIdx + 1);
        long regionSize2 = getRegionSize(hri2);
        if (regionSize >= 0 && regionSize2 >= 0 && regionSize + regionSize2 < avgRegionSize) {
          LOG.info(
            "Table " + table + ", small region size: " + regionSize + " plus its neighbor size: "
                + regionSize2 + ", less than the avg size " + avgRegionSize + ", merging them");
          plans.add(new MergeNormalizationPlan(hri, hri2));
          candidateIdx++;
        }

      }
      candidateIdx++;
    }
    if (plans.isEmpty()) {
      LOG.debug("No normalization needed, regions look good for table: " + table);
    }
    Collections.sort(plans, planComparator);
    return plans;

  }

  private long getRegionSize(HRegionInfo hri) {
    ServerName sn = masterServices.getAssignmentManager().getRegionStates().
      getRegionServerOfRegion(hri);
    RegionLoad regionLoad = masterServices.getServerManager().getLoad(sn).
      getRegionsLoad().get(hri.getRegionName());
    return regionLoad.getStorefileSizeMB();
  }
}

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
package org.apache.hadoop.hbase.regionserver;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A custom RegionSplitPolicy implementing a SplitPolicy that groups
 * rows by the first item of the row-key. The row keys are separated by a user defined 
 * delimiter.
 * This ensures that a region is not split "inside" row keys with same first item.
 * I.e. rows can be co-located in a region by first item of a row key
 */
@InterfaceAudience.Private
public class KeyDelimiterPrefixRegionSplitPolicy extends ConstantSizeRegionSplitPolicy {
  static final Log LOG = LogFactory
      .getLog(KeyDelimiterPrefixRegionSplitPolicy.class);
  
  public static String DELIMITER_KEY = "delimiter_prefix_split_key_policy.delimiter";

  private byte delimiter = '-';

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    if (region != null) {
      // read the prefix length from the table descriptor
      String delimiterString = region.getTableDesc().getValue(
        DELIMITER_KEY);
      if (delimiterString == null || delimiterString.length() != 1) {
        LOG.error(DELIMITER_KEY + " not specified for table "
            + region.getTableDesc().getNameAsString()
            + ". Using default delimiter: '-'");
        return;
      }
      delimiter = Bytes.toBytes(delimiterString)[0];
    }
  }

  @Override
  protected byte[] getSplitPoint() {
    byte[] splitPoint = super.getSplitPoint();
    if (splitPoint != null && splitPoint.length > 0) {
      int end = com.google.common.primitives.Bytes.indexOf(splitPoint, delimiter);
      if (end == -1) return splitPoint;
      return Arrays.copyOf(splitPoint, end + 1);
    }
    return null;
  }
}

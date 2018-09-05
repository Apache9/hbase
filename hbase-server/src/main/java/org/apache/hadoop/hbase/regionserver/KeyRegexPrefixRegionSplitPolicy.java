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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom RegionSplitPolicy implementing a SplitPolicy that groups rows by a prefix of the
 * row-key. The prefix is chosen according to regular expression match. This ensures that a region
 * is not split "inside" a prefix of a row key. I.e. rows can be co-located in a region by their
 * prefix.
 */
@InterfaceAudience.Private
public class KeyRegexPrefixRegionSplitPolicy extends ConstantSizeRegionSplitPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(KeyPrefixRegionSplitPolicy.class);
  public static final String PREFIX_REGEX_KEY = "prefix_split_key_policy.prefix_regex";

  private Pattern prefixPattern = null;

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    // read the prefix regex from the table descriptor
    String prefixRegexString = region.getTableDescriptor().getValue(PREFIX_REGEX_KEY);
    if (prefixRegexString == null) {
      LOG.error(PREFIX_REGEX_KEY + " not specified for table "
          + region.getTableDescriptor().getTableName().getNameAsString()
          + ". Using default RegionSplitPolicy");
      return;
    }
    try {
      prefixPattern = Pattern.compile(prefixRegexString);
    } catch (PatternSyntaxException pse) {
      LOG.error("Invalid value for " + PREFIX_REGEX_KEY + " for table "
          + region.getTableDescriptor().getTableName().getNameAsString() + ":" + prefixRegexString
          + ". Using default RegionSplitPolicy");
    }
  }

  @Override
  protected byte[] getSplitPoint() {
    byte[] splitPoint = super.getSplitPoint();
    if (prefixPattern != null && splitPoint != null && splitPoint.length > 0) {

      try {
        // ISO-8859-1 maps each possible byte to a char
        String s = new String(splitPoint, "ISO-8859-1");
        Matcher m = prefixPattern.matcher(s);
        if (m.find()) {
          int prefixLength = m.group().length();
          // group split keys by a prefix
          return Arrays.copyOf(splitPoint, Math.min(prefixLength, splitPoint.length));
        }
      } catch (UnsupportedEncodingException e) {
        // ignore
      }
    }

    return splitPoint;
  }
}

package org.apache.hadoop.hbase.regionserver;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A custom RegionSplitPolicy implementing a SplitPolicy that groups rows by a prefix of the
 * row-key. The prefix is chosen according to regular expression match.
 * 
 * This ensures that a region is not split "inside" a prefix of a row key. I.e. rows can be
 * co-located in a region by their prefix.
 */
public class KeyRegexPrefixRegionSplitPolicy extends IncreasingToUpperBoundRegionSplitPolicy {
  private static final Log LOG = LogFactory.getLog(KeyPrefixRegionSplitPolicy.class);
  public static String PREFIX_REGEX_KEY = "prefix_split_key_policy.prefix_regex";

  private Pattern prefixPattern = null;

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    if (region != null) {
      // read the prefix regex from the table descriptor
      String prefixRegexString = region.getTableDesc().getValue(PREFIX_REGEX_KEY);
      if (prefixRegexString == null) {
        LOG.error(PREFIX_REGEX_KEY + " not specified for table "
            + region.getTableDesc().getNameAsString()
            + ". Using default RegionSplitPolicy");
        return;
      }
      try {
        prefixPattern = Pattern.compile(prefixRegexString);
      } catch (PatternSyntaxException pse) {
        LOG.error("Invalid value for " + PREFIX_REGEX_KEY + " for table "
            + region.getTableDesc().getNameAsString() + ":"
            + prefixRegexString + ". Using default RegionSplitPolicy");
      }
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
          return Arrays.copyOf(splitPoint,
            Math.min(prefixLength, splitPoint.length));
        }
      } catch (UnsupportedEncodingException e) {
        // ignore
      }
    }

    return splitPoint;
  }
}

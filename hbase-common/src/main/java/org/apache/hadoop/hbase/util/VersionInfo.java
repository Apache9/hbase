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

package org.apache.hadoop.hbase.util;

import java.io.PrintWriter;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.VersionAnnotation;
import org.apache.commons.logging.Log;

/**
 * This class finds the package info for hbase and the VersionAnnotation
 * information.  Taken from hadoop.  Only name of annotation is different.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class VersionInfo {
  private static final Log LOG = LogFactory.getLog(VersionInfo.class.getName());
  private static Package myPackage;
  private static VersionAnnotation version;

  static {
    myPackage = VersionAnnotation.class.getPackage();
    version = myPackage.getAnnotation(VersionAnnotation.class);
  }

  /**
   * Get the meta-data for the hbase package.
   * @return package
   */
  static Package getPackage() {
    return myPackage;
  }

  /**
   * Get the hbase version.
   * @return the hbase version string, eg. "0.6.3-dev"
   */
  public static String getVersion() {
    return version != null ? version.version() : "Unknown";
  }

  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static String getRevision() {
    return version != null ? version.revision() : "Unknown";
  }

  /**
   * The date that hbase was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return version != null ? version.date() : "Unknown";
  }

  /**
   * The user that compiled hbase.
   * @return the username of the user
   */
  public static String getUser() {
    return version != null ? version.user() : "Unknown";
  }

  /**
   * Get the subversion URL for the root hbase directory.
   * @return the url
   */
  public static String getUrl() {
    return version != null ? version.url() : "Unknown";
  }

  static String[] versionReport() {
    return new String[] {
      "HBase " + getVersion(),
      "Source code repository " + getUrl() + " revision=" + getRevision(),
      "Compiled by " + getUser() + " on " + getDate(),
      "From source with checksum " + getSrcChecksum()
      };
  }

  /**
   * Get the checksum of the source files from which Hadoop was compiled.
   * @return a string that uniquely identifies the source
   **/
  public static String getSrcChecksum() {
    return version != null ? version.srcChecksum() : "Unknown";
  }

  public static void writeTo(PrintWriter out) {
    for (String line : versionReport()) {
      out.println(line);
    }
  }

  public static void logVersion() {
    for (String line : versionReport()) {
      LOG.info(line);
    }
  }

  public static void main(String[] args) {
    logVersion();
  }

  public static int compareVersion(String v1, String v2) {
    //fast compare equals first
    if (v1.equals(v2)) {
      return 0;
    }

    String communityVer1 = "", communityVer2 = "", mdhV1 = "", mdhV2 = "";
    String[] twoPart = v1.split("-mdh");
    communityVer1 = twoPart[0];
    if (twoPart.length > 1) {
      mdhV1 = twoPart[1];
    }
    twoPart = v2.split("-mdh");
    communityVer2 = twoPart[0];
    if (twoPart.length > 1) {
      mdhV2 = twoPart[1];
    }

    int c = compareSemanticVersion(communityVer1, communityVer2);
    if (c != 0) {
      return c;
    }
    return compareSemanticVersion(mdhV1, mdhV2);
  }

  private static int compareSemanticVersion(String v1, String v2) {
    //fast compare equals first
    if (v1.equals(v2)) {
      return 0;
    }

    String s1[] = v1.split("\\.|-");//1.2.3-hotfix -> [1, 2, 3, hotfix]
    String s2[] = v2.split("\\.|-");
    int index = 0;
    while (index < s1.length && index < s2.length) {
      int va = 10000, vb = 10000;
      try {
        va = Integer.parseInt(s1[index]);
      } catch (Exception ingore) {
      }
      try {
        vb = Integer.parseInt(s2[index]);
      } catch (Exception ingore) {
      }
      if (va != vb) {
        return va - vb;
      }
      if (va == 10000) {
        // compare as String
        int c = s1[index].compareTo(s2[index]);
        if (c != 0) {
          return c;
        }
      }
      index++;
    }
    if (index < s1.length) {
      // s1 is longer
      return 1;
    }
    //s2 is longer
    return -1;
  }

}

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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.FamilyFiles;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * A client scanner for a region opened for read-only on the client side. Assumes region data
 * is not changing.
 */
@InterfaceAudience.Private
public class ClientSideRegionScanner extends AbstractClientScanner {
  private static final Log LOG = LogFactory.getLog(ClientSideRegionScanner.class);

  private HRegion region;
  private Scan scan;
  RegionScanner scanner;
  List<Cell> values;

  public ClientSideRegionScanner(Configuration conf, FileSystem fs, Path rootDir,
      HTableDescriptor htd, HRegionInfo hri, Scan scan, ScanMetrics scanMetrics,
      SnapshotRegionManifest snapshotRegionManifest) throws IOException {

    this.scan = scan;

    // region is immutable, set isolation level
    scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);

    htd.setReadOnly(true);

    // open region from the snapshot directory
    this.region = HRegion.openHRegion(conf, fs, rootDir, hri, htd, null, null, null);
    // check store files between restore directory and snapshot manifest
    checkStoreFiles(region, snapshotRegionManifest, rootDir);

    // create an internal region scanner
    this.scanner = region.getScanner(scan);
    values = new ArrayList<Cell>();

    if (scanMetrics == null) {
      initScanMetrics(scan);
    } else {
      this.scanMetrics = scanMetrics;
    }
    region.startRegionOperation();
  }

  private void checkStoreFiles(HRegion region, SnapshotRegionManifest snapshotRegionManifest,
      Path rootDir) throws IOException {
    if (snapshotRegionManifest != null) {
      Map<byte[], Store> regionStores = region.getStores();
      String encodedRegionName = region.getRegionInfo().getEncodedName();
      Path tableDir = FSUtils.getTableDir(rootDir, region.getTableDesc().getTableName());
      Path regionDir = new Path(tableDir, encodedRegionName);
      for (FamilyFiles snapshotFamilyFiles : snapshotRegionManifest.getFamilyFilesList()) {
        // family
        ByteString snapshotFamily = snapshotFamilyFiles.getFamilyName();
        Path familyDir = new Path(regionDir, Bytes.toString(snapshotFamily.toByteArray()));
        // reference files read from snapshot manifest
        Set<String> snapshotStoreFiles = snapshotFamilyFiles.getStoreFilesList().stream()
            .map(storeFile -> getHFileName(new Path(familyDir, storeFile.getName())))
            .filter(path -> path != null).map(path -> path.getName()).collect(Collectors.toSet());
        // reference files read from restore directory
        if (regionStores.get(snapshotFamily.toByteArray()) == null) {
          throw new IOException("Family {" + snapshotFamily.toStringUtf8()
              + "} is not in restored region but in snapshot manifest, region{"
              + region.getRegionInfo().getRegionNameAsString() + "}, restoreDir {"
              + rootDir.toString() + "}");
        }
        Set<String> restoredStoreFiles = regionStores.get(snapshotFamily.toByteArray())
            .getStorefiles().stream().map(sf -> getHFileName(sf.getPath()))
            .filter(path -> path != null).map(path -> path.getName()).collect(Collectors.toSet());
        for (String snapshotStoreFile : snapshotStoreFiles) {
          if (!restoredStoreFiles.contains(snapshotStoreFile)) {
            throw new IOException("Storefile {" + snapshotStoreFile
                + "} is not in restored region but in snapshot manifest, region {"
                + region.getRegionInfo().getRegionNameAsString() + "}, column {"
                + snapshotFamily.toStringUtf8() + "}, restoreDir {" + rootDir.toString() + "}");
          }
        }
      }
    } else {
      LOG.error(
        "snapshot region manifest is NULL and skip check storefiles when open a restored region");
    }
  }

  private Path getHFileName(Path path) {
    if (HFileLink.isHFileLink(path)) {
      String hfile = path.getName();
      String referencedHfile = HFileLink.getReferencedHFileName(hfile);
      String referencedRegion = HFileLink.getReferencedRegionName(hfile);
      TableName referencedTable = HFileLink.getReferencedTableName(hfile);
      return new Path(new Path(
          new Path(new Path(referencedTable.getNamespaceAsString(),
              referencedTable.getQualifierAsString()), referencedRegion),
          path.getParent().getName()), referencedHfile);
    } else if (StoreFileInfo.isReference(path)) {
      // return StoreFileInfo.getReferredToFile(path);
      // TODO return null because this is another region's file, so the restored region does not
      // contain this file
      return null;
    } else {
      return path;
    }
  }

  @Override
  public Result next() throws IOException {
    values.clear();

    scanner.nextRaw(values);

    if (values == null || values.isEmpty()) {
      //we are done
      return null;
    }

    Result result = Result.create(values);
    if (this.scanMetrics != null) {
      long resultSize = 0;
      for (Cell kv : values) {
        // TODO add getLength to Cell/use CellUtil#estimatedSizeOf
        resultSize += KeyValueUtil.ensureKeyValue(kv).getLength();
      }
      this.scanMetrics.countOfBytesInResults.addAndGet(resultSize);
    }

    return result;
  }

  @Override
  public void close() {
    if (this.scanner != null) {
      try {
        this.scanner.close();
        this.scanner = null;
      } catch (IOException ex) {
        LOG.warn("Exception while closing scanner", ex);
      }
    }
    if (this.region != null) {
      try {
        this.region.closeRegionOperation();
        this.region.close(true);
        this.region = null;
      } catch (IOException ex) {
        LOG.warn("Exception while closing region", ex);
      }
    }
  }

  @Override
  public boolean renewLease() {
    throw new UnsupportedOperationException();
  }
}

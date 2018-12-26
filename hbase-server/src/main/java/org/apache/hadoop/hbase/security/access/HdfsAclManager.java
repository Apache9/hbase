/*
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

package org.apache.hadoop.hbase.security.access;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

@InterfaceAudience.Private
public class HdfsAclManager {

  private static Log LOG = LogFactory.getLog(HdfsAclManager.class);
  private Configuration conf;
  private ExecutorService pool = null;
  private PathHelper pathHelper = null;
  private FileSystem fs = null;

  public HdfsAclManager(Configuration conf) {
    this.conf = conf;
  }

  public void start() {
    pool = Executors.newFixedThreadPool(conf.getInt(HConstants.HDFS_ACL_THREAD_NUMBER, 10),
            new ThreadFactoryBuilder().setNameFormat("hdfs-acl-manager-thread-%d").build());
    pathHelper = new PathHelper(conf);
    fs = pathHelper.getFileSystem();
  }

  public void stop() {
    if (pool != null) {
      pool.shutdown();
    }
  }

  public void grantAcl(UserPermission userPerm, AccessControlProtos.Permission.Type type,
      List<UserPermission> previousPerms) {
    try {
      // if user already has read perm, skip set acl
      if (previousPerms != null
          && previousPerms.stream().filter(p -> Bytes.equals(p.getUser(), userPerm.getUser()))
              .anyMatch(p -> !p.hasFamily() && hasReadPerm(p))) {
        return;
      }
      // if grant to cf or cq, skip set acl
      if (type == AccessControlProtos.Permission.Type.Table
          && (userPerm.hasFamily() || userPerm.hasQualifier())) {
        return;
      }
      if (hasReadPerm(userPerm)) {
        long start = System.currentTimeMillis();
        List<PathAcl> pathAcls = getGrantOrRevokePathAcls(userPerm, AclType.MODIFY, type);
        traverseAndSetAcl(pathAcls);
        traverseAndSetAcl(pathAcls); // set acl twice in case of file move
        LOG.info("set acl when grant: " + userPerm.toString() + ", cost " + (System.currentTimeMillis() - start) + " ms.");
      }
    } catch (Exception e) {
      LOG.error("set acl error when grant: " + (userPerm != null ? userPerm.toString() : null), e);
    }
  }

  public void revokeAcl(UserPermission userPerm, AccessControlProtos.Permission.Type type) {
    try {
      // if user has ns read perm, skip revoke table
      if (type == AccessControlProtos.Permission.Type.Table
              && userHasNamespaceReadPermission(Bytes.toString(userPerm.getUser()), userPerm.getTableName().getNamespaceAsString())) {
          return;
      }
      // revoke command does not contain permission
      long start = System.currentTimeMillis();
      List<PathAcl> pathAcls = getGrantOrRevokePathAcls(userPerm, AclType.REMOVE, type);
      traverseAndSetAcl(pathAcls);
      traverseAndSetAcl(pathAcls); // set acl twice in case of file move
      LOG.info("set acl when revoke: " + userPerm.toString() + ", cost " + (System.currentTimeMillis() - start) + " ms.");
      // if user has table read perm when revoke ns perm, then reset table acl
      if (type == AccessControlProtos.Permission.Type.Namespace) {
        resetNamespaceTableAcl(Bytes.toString(userPerm.getUser()), userPerm.getNamespace());
      }
    } catch (Exception e) {
      LOG.error("set acl error when revoke: " + (userPerm != null ? userPerm.toString() : null), e);
    }
  }

  public void snapshotAcl(SnapshotProtos.SnapshotDescription snapshot) {
    try {
      long start = System.currentTimeMillis();
      TableName tableName = TableName.valueOf(snapshot.getTable());
      Set<String> userSet = new HashSet<>();
      userSet.addAll(AccessControlLists.getTablePermissions(conf, tableName).keySet());
      userSet.addAll(AccessControlLists
              .getNamespacePermissions(conf, tableName.getNamespaceAsString()).keySet());
      List<String> users = Lists.newArrayList(userSet);

      List<Path> pathList = Lists.newArrayList(pathHelper.getSnapshotDir(snapshot.getName()));
      List<PathAcl> pathAcls = getDefaultPathAcls(pathList, users, AclType.MODIFY);
      traverseAndSetAcl(pathAcls);
      traverseAndSetAcl(pathAcls); // set acl twice in case of file move
      LOG.info("set acl when snapshot: " + snapshot.getName() + ", cost " + (System.currentTimeMillis() - start) + " ms.");
    } catch (Exception e) {
      LOG.error("set acl error when snapshot: " + (snapshot != null ? snapshot.getName() : null),
        e);
    }
  }

  /**
   * When truncate table, data/ns/table/region directory is moved and recreated. So reset
   * data/ns/table/region directory permission.
   */
  public void resetAcl(TableName tableName) {
    try {
      long start = System.currentTimeMillis();
      List<String> users =
        Lists.newArrayList(AccessControlLists.getTablePermissions(conf, tableName).keySet());
      List<Path> defaultPathList = Lists.newArrayList(pathHelper.getTmpTableDir(tableName));
      List<PathAcl> pathAcls = getDefaultPathAcls(defaultPathList, users, AclType.MODIFY);

      traverseAndSetAcl(pathAcls);
      traverseAndSetAcl(pathAcls); // set acl twice in case of file move
      LOG.info("set acl when truncate table: " + tableName.getNameAsString() + ", cost " + (System.currentTimeMillis() - start) + " ms.");
    } catch (Exception e) {
      LOG.error("set acl error when truncate table: "
          + (tableName != null ? tableName.getNameAsString() : null),
        e);
    }
  }

  private boolean userHasNamespaceReadPermission(String user, String namespace) throws IOException {
    ListMultimap<String, TablePermission> permissions = AccessControlLists
            .getNamespacePermissions(conf, namespace);
    return permissions.entries().stream().anyMatch(entry ->
            entry.getKey().equals(user) && hasReadPerm(entry.getValue()));
  }

  // when user has read perm on tables of ns, then reset table user acl.
  private void resetNamespaceTableAcl(String user, String namespace) throws IOException {
    AccessControlLists.loadAll(conf).values().forEach(userTablePermMap -> userTablePermMap.entries().forEach(userTablePermEntry -> {
      if (userTablePermEntry.getKey().equals(user)) {
        TablePermission tablePermission = userTablePermEntry.getValue();
        if (tablePermission.hasTable() && !tablePermission.hasFamily()
                && tablePermission.getTableName().getNamespaceAsString().equals(namespace)
                && hasReadPerm(tablePermission)) {
          grantAcl(new UserPermission(Bytes.toBytes(user), tablePermission), AccessControlProtos.Permission.Type.Table, null);
          LOG.info("reset table acl because revoke ns: " +namespace+", table: "+tablePermission.getTableName().getNameAsString());
        }
      }})
    );
  }

  private List<PathAcl> getGrantOrRevokePathAcls(UserPermission userPerm, AclType op,
      AccessControlProtos.Permission.Type type) throws IOException {
    List<String> users = Lists.newArrayList(Bytes.toString(userPerm.getUser()));
    List<PathAcl> pathAcls = new ArrayList<>();
    switch (type) {
    case Global:
      List<Path> defaultGlobalPathList = Lists.newArrayList(pathHelper.getTmpDataDir(),
        pathHelper.getDataDir(), pathHelper.getSnapshotRootDir(), pathHelper.getArchiveDataDir());
      pathAcls = getDefaultPathAcls(defaultGlobalPathList, users, op);
      break;
    case Namespace:
      String namespace = userPerm.getNamespace();
      List<Path> defaultNsPathList = Lists.newArrayList(pathHelper.getTmpNsDir(namespace),
        pathHelper.getNsDir(namespace), pathHelper.getArchiveNsDir(namespace));
      defaultNsPathList.addAll(getSnapshots(namespace, false).stream()
          .map(snap -> pathHelper.getSnapshotDir(snap)).collect(Collectors.toList()));
      pathAcls = getDefaultPathAcls(defaultNsPathList, users, op);
      break;
    case Table:
      TableName tableName = userPerm.getTableName();
      String tableNamespace = tableName.getNamespaceAsString();

      List<Path> tablePathList = Lists.newArrayList(pathHelper.getTmpNsDir(tableNamespace),
        pathHelper.getNsDir(tableNamespace), pathHelper.getArchiveNsDir(tableNamespace));
      pathAcls = getPathAcls(tablePathList, users, op);

      List<Path> defaultTablePathList =
          Lists.newArrayList(pathHelper.getTmpTableDir(tableName),
            pathHelper.getTableDir(tableName),
            pathHelper.getArchiveTableDir(tableName));
      defaultTablePathList.addAll(getSnapshots(tableName.getNameAsString(), true).stream()
          .map(snap -> pathHelper.getSnapshotDir(snap)).collect(Collectors.toList()));
      pathAcls.addAll(getDefaultPathAcls(defaultTablePathList, users, op));
      break;
    }
    return pathAcls;
  }

  private List<PathAcl> getPathAcls(List<Path> pathList, List<String> users, AclType op) {
    List<AclEntry> aclList = new ArrayList<>();
    for (String user : users) {
      aclList.add(aclEntry(ACCESS, user));
    }
    HdfsAclManager.AclOperation aclOperation = null;
    switch (op) {
    case MODIFY:
      aclOperation = (fileSystem, modifyPath, acls) -> fileSystem
          .modifyAclEntries(modifyPath, acls);
      break;
    case REMOVE:
      aclOperation = (fileSystem, removePath, acls) -> fileSystem
          .removeAclEntries(removePath, acls);
      break;
    }

    List<PathAcl> pathAcls = new ArrayList<>();
    for (Path path : pathList) {
      PathAcl pathAcl = new PathAcl(fs, path, aclList, aclList, aclOperation, false);
      pathAcls.add(pathAcl);
    }
    return pathAcls;
  }

  private List<PathAcl> getDefaultPathAcls(List<Path> pathList, List<String> users,
      AclType op) {
    List<AclEntry> dirAclList = new ArrayList<>();
    List<AclEntry> fileAclList = new ArrayList<>();
    for (String user : users) {
      dirAclList.add(aclEntry(ACCESS, user));
      dirAclList.add(aclEntry(DEFAULT, user));
      fileAclList.add(aclEntry(ACCESS, user));
    }
    HdfsAclManager.AclOperation aclOperation = null;
    switch (op) {
      case MODIFY:
        aclOperation = (fileSystem, modifyPath, aclList) -> fileSystem
                .modifyAclEntries(modifyPath, aclList);
        break;
      case REMOVE:
        aclOperation = (fileSystem, removePath, aclList) -> fileSystem
                .removeAclEntries(removePath, aclList);
        break;
    }

    List<PathAcl> pathAcls = new ArrayList<>();
    for (Path path : pathList) {
      pathAcls.add(new PathAcl(fs, path, dirAclList, fileAclList, aclOperation, true));
    }
    return pathAcls;
  }

  private void traverseAndSetAcl(List<PathAcl> pathAcls) {
    final BlockingQueue<PathAcl> queue = new LinkedBlockingQueue<>();
    AtomicLong pathCount = new AtomicLong(pathAcls.size());
    pathAcls.forEach(queue::add);

    while (pathCount.get() > 0) {
      PathAcl pathAcl = queue.poll();
      if (pathAcl != null) {
        pool.submit(() -> {
          try {
            pathAcl.setAcl();
            List<PathAcl> paths = pathAcl.getChildPathAcls();
            queue.addAll(paths);
            pathCount.addAndGet(paths.size());
          } catch (Exception e) {
            if (pathAcl != null && pathAcl.path != null) {
              LOG.error("Set hdfs acl error for path: " + pathAcl.path.toString(), e);
            } else {
              LOG.error("Set hdfs acl error because of NPE", e);
            }
          } finally {
            pathCount.decrementAndGet();
          }
          return null;
        });
      }
    }
  }

  private AclEntry aclEntry(AclEntryScope scope, String name) {
    return new AclEntry.Builder().setScope(scope).setType(USER).setName(name)
        .setPermission(READ_EXECUTE).build();
  }

  private List<String> getSnapshots(String name, boolean isTable) throws IOException{
    List<String> snapshots = new ArrayList<>();
    List<SnapshotProtos.SnapshotDescription> snapshotDescriptions = SnapshotManager
      .getCompletedSnapshots(fs, pathHelper.getRootDir(), pathHelper.getSnapshotRootDir());
    for (SnapshotProtos.SnapshotDescription snapshot : snapshotDescriptions) {
      if (isTable) {
        if (snapshot.getTable().equals(name)) {
          snapshots.add(snapshot.getName());
        }
      } else { // namespace
        if (snapshot.getTable().startsWith(name + ":")) {
          snapshots.add(snapshot.getName());
        }
      }
    }
    return snapshots;
  }

  //used for PresetHdfsAclTool
  FileSystem getFileSystem() {
    return fs;
  }

  PathHelper getPathHelper() {
    return pathHelper;
  }

  private enum AclType {
    MODIFY, REMOVE
  }

  private interface AclOperation {
    void apply(FileSystem fs, Path path, List<AclEntry> aclList) throws IOException;
  }

  private class PathAcl {
    private FileSystem fs;
    private Path path;
    private List<AclEntry> dirAcl;
    private List<AclEntry> fileAcl;
    private AclOperation aclOperation;
    private boolean recursive;

    PathAcl(FileSystem fs, Path path, List<AclEntry> dirAcl, List<AclEntry> fileAcl,
        AclOperation aclOperation, boolean recursive) {
      this.fs = fs;
      this.path = path;
      this.dirAcl = dirAcl;
      this.fileAcl = fileAcl;
      this.aclOperation = aclOperation;
      this.recursive = recursive;
    }

    PathAcl(Path path, PathAcl parent) {
      this.fs = parent.fs;
      this.path = path;
      this.dirAcl = parent.dirAcl;
      this.fileAcl = parent.fileAcl;
      this.aclOperation = parent.aclOperation;
      this.recursive = parent.recursive;
    }

    List<PathAcl> getChildPathAcls() throws IOException{
      List<PathAcl> pathAcls = new ArrayList<>();
      if (recursive && fs.isDirectory(path)) {
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
          pathAcls.add(new PathAcl(fileStatus.getPath(), this));
        }
      }
      return pathAcls;
    }

    void setAcl() throws IOException {
      if (fs.exists(path)) {
        if (fs.isDirectory(path)) {
          aclOperation.apply(fs, path, dirAcl);
        } else {
          aclOperation.apply(fs, path, fileAcl);
        }
      }
    }
  }

  private boolean hasReadPerm(TablePermission permission) {
    return Stream.of(permission.actions).anyMatch(a -> a == Permission.Action.READ);
  }

  static class PathHelper {
    Configuration conf;
    Path rootDir;
    Path tmpDir;
    Path tmpDataDir;
    Path dataDir;
    Path archiveDir;
    Path archiveDataDir;
    Path snapshotDir;

    PathHelper(Configuration conf) {
      this.conf = conf;
      rootDir = new Path(conf.get(HConstants.HBASE_DIR));
      tmpDir = new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY);
      tmpDataDir = new Path(tmpDir, HConstants.BASE_NAMESPACE_DIR);
      dataDir = new Path(rootDir, HConstants.BASE_NAMESPACE_DIR);
      archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
      archiveDataDir = new Path(archiveDir, HConstants.BASE_NAMESPACE_DIR);
      snapshotDir = new Path(rootDir, HConstants.SNAPSHOT_DIR_NAME);
    }

    Path getRootDir() {
      return rootDir;
    }

    Path getDataDir() {
      return dataDir;
    }

    Path getTmpDir() {
      return tmpDir;
    }

    Path getTmpDataDir() {
      return tmpDataDir;
    }

    Path getArchiveDir() {
      return archiveDir;
    }

    Path getArchiveDataDir() {
      return archiveDataDir;
    }

    Path getNsDir(String namespace) {
      return new Path(dataDir, namespace);
    }

    Path getTableDir(TableName tableName) {
      return new Path(getNsDir(tableName.getNamespaceAsString()), tableName.getQualifierAsString());
    }

    Path getArchiveNsDir(String namespace) {
      return new Path(archiveDataDir, namespace);
    }

    Path getArchiveTableDir(TableName tableName) {
      return new Path(getArchiveNsDir(tableName.getNamespaceAsString()), tableName.getQualifierAsString());
    }

    Path getTmpNsDir(String namespace) {
      return new Path(tmpDataDir, namespace);
    }

    Path getTmpTableDir(TableName tableName) {
      return new Path(getTmpNsDir(tableName.getNamespaceAsString()), tableName.getQualifierAsString());
    }

    Path getSnapshotRootDir() {
      return snapshotDir;
    }

    Path getSnapshotDir(String snapshot) {
      return new Path(snapshotDir, snapshot);
    }

    FileSystem getFileSystem() {
      try {
        return rootDir.getFileSystem(conf);
      } catch (IOException e) {
        LOG.error(e);
        return null;
      }
    }
  }

  /**
   * If hbase.hdfs.acl is enabled, check exists (create if not exists) and set permission for public
   * directories.
   * @param conf The configuration of the cluster
   * @param fs The file system
   * @throws IOException
   */
  public static void setPublicDirectoryPermission(Configuration conf, FileSystem fs)
      throws IOException {
    if (conf.getBoolean(HConstants.HDFS_ACL_ENABLE, false)) {
      PathHelper pathHelper = new PathHelper(conf);
      String owner = fs.getFileStatus(pathHelper.getRootDir()).getOwner();
      Path[] publicDirs = new Path[] { pathHelper.getRootDir(), pathHelper.getDataDir(),
          pathHelper.getTmpDir(), pathHelper.getTmpDataDir(), pathHelper.getArchiveDir(),
          pathHelper.getArchiveDataDir(), pathHelper.getSnapshotRootDir() };
      checkDirExistAndSetPermission(fs, getHdfsAclEnabledPublicFilePermission(conf), owner,
        publicDirs);
      LOG.info("Finish set public directories permission used for scanning snapshot.");
    }
  }

  /**
   * If hbase.hdfs.acl is enabled, check exists (create if not exists) and set permission for
   * restore directory.
   * @param conf The configuration of the cluster
   * @param fs The file system
   */
  public static void setRestoreDirectoryPermission(Configuration conf, FileSystem fs) {
    if (conf.getBoolean(HConstants.HDFS_ACL_ENABLE, false)) {
      try {
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        String owner = fs.getFileStatus(rootDir).getOwner();
        Path restoreDir = new Path(conf.get(HConstants.SNAPSHOT_RESTORE_TMP_DIR,
          HConstants.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT));
        checkDirExistAndSetPermission(fs, getHdfsAclEnabledRestoreFilePermission(conf), owner,
          restoreDir);
        // set hbase owner acl over restore directory to run restore hfile cleaner
        AclEntry ownerAccessAclEntry = new AclEntry.Builder().setScope(ACCESS).setType(USER)
            .setName(owner).setPermission(ALL).build();
        AclEntry ownerDefaultAclEntry = new AclEntry.Builder().setScope(DEFAULT).setType(USER)
            .setName(owner).setPermission(ALL).build();
        fs.modifyAclEntries(restoreDir,
          Lists.newArrayList(ownerAccessAclEntry, ownerDefaultAclEntry));
        LOG.info("Finish set restore directory permission used for scanning snapshot.");
      } catch (IOException e) {
        LOG.warn("Failed to set restore directory permission used for scanning snapshots", e);
      }
    }
  }

  private static void checkDirExistAndSetPermission(FileSystem fs, FsPermission permission,
      String owner, Path... dirPaths) throws IOException {
    for (Path path : dirPaths) {
      if (!fs.exists(path)) {
        fs.mkdirs(path);
      }
      if (!fs.getFileStatus(path).getOwner().equals(owner)) {
        throw new IOException("The owner of directory [" + path.toString() + "] must be " + owner
            + ", please change the owner manually.");
      }
      fs.setPermission(path, permission);
    }
  }

  @VisibleForTesting
  protected static FsPermission getHdfsAclEnabledPublicFilePermission(Configuration conf) {
    return new FsPermission(conf.get(HConstants.HDFS_ACL_ENABLED_PUBLIC_HFILE_PERMISSION,
      HConstants.HDFS_ACL_ENABLED_PUBLIC_HFILE_PERMISSION_DEFAULT));
  }

  @VisibleForTesting
  protected static FsPermission getHdfsAclEnabledRestoreFilePermission(Configuration conf) {
    return new FsPermission(conf.get(HConstants.HDFS_ACL_ENABLED_RESTORE_HFILE_PERMISSION,
      HConstants.HDFS_ACL_ENABLED_RESTORE_HFILE_PERMISSION_DEFAULT));
  }
}

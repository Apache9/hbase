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
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
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
    pool = Executors.newFixedThreadPool(conf.getInt(HConstants.HDFS_ACL_THREAD_NUMBER, 10));
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
              .anyMatch(p -> Stream.of(p.actions).anyMatch(a -> a == Permission.Action.READ))) {
        return;
      }
      // if grant to cf or cq, skip set acl
      if (type == AccessControlProtos.Permission.Type.Table
          && (userPerm.hasFamily() || userPerm.hasQualifier())) {
        return;
      }
      for (Permission.Action action : userPerm.getActions()) {
        if (action == Permission.Action.READ) {
          List<PathAcl> pathAcls = getGrantOrRevokePathAcls(userPerm, AclType.MODIFY, type);
          traverseAndSetAcl(pathAcls);
          traverseAndSetAcl(pathAcls); // set acl twice in case of file move
          LOG.info("set acl when grant: " + userPerm.toString());
          break;
        }
      }
    } catch (Exception e) {
      LOG.error("set acl error when grant: " + (userPerm != null ? userPerm.toString() : null), e);
    }
  }

  public void revokeAcl(UserPermission userPerm, AccessControlProtos.Permission.Type type) {
    try {
      // revoke command does not contain permission
      List<PathAcl> pathAcls = getGrantOrRevokePathAcls(userPerm, AclType.REMOVE, type);
      traverseAndSetAcl(pathAcls);
      traverseAndSetAcl(pathAcls); // set acl twice in case of file move
      LOG.info("set acl when revoke: " + userPerm.toString());
    } catch (Exception e) {
      LOG.error("set acl error when revoke: " + (userPerm != null ? userPerm.toString() : null), e);
    }
  }

  public void snapshotAcl(SnapshotProtos.SnapshotDescription snapshot) {
    try {
      TableName tableName = TableName.valueOf(snapshot.getTable());
      List<String> users = new ArrayList<>();
      users.addAll(AccessControlLists.getTablePermissions(conf, tableName).keySet());
      users.addAll(AccessControlLists
          .getNamespacePermissions(conf, tableName.getNamespaceAsString()).keySet());

      List<Path> pathList = Lists.newArrayList(pathHelper.getSnapshotDir(snapshot.getName()));
      List<PathAcl> pathAcls = getDefaultPathAcls(pathList, users, AclType.MODIFY);
      traverseAndSetAcl(pathAcls);
      traverseAndSetAcl(pathAcls); // set acl twice in case of file move
      LOG.info("set acl when snapshot: " + snapshot.getName());
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
      List<String> users =
        Lists.newArrayList(AccessControlLists.getTablePermissions(conf, tableName).keySet());
      List<Path> defaultPathList = Lists.newArrayList(pathHelper.getTmpTableDir(tableName));
      List<PathAcl> pathAcls = getDefaultPathAcls(defaultPathList, users, AclType.MODIFY);

      traverseAndSetAcl(pathAcls);
      traverseAndSetAcl(pathAcls); // set acl twice in case of file move
      LOG.info("set acl when truncate table: " + tableName.getNameAsString());
    } catch (Exception e) {
      LOG.error("set acl error when truncate table: "
          + (tableName != null ? tableName.getNameAsString() : null),
        e);
    }
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

  private List<PathAcl> getPathAcls(List<Path> pathList, List<String> users, AclType op)
      throws IOException {
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
      AclType op) throws IOException {
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

  private void traverseAndSetAcl(List<PathAcl> pathAcls) throws Exception {
    Queue<PathAcl> queue = new LinkedList<>();
    List<Future<Void>> tasks = new ArrayList<>();

    pathAcls.forEach(queue::add);

    while (!queue.isEmpty()) {
      PathAcl pathAcl = queue.poll();
      tasks.add(pool.submit(() -> {
        pathAcl.setAcl();
        return null;
      }));
      queue.addAll(pathAcl.getChildPathAcls());
    }

    for (Future<Void> task : tasks) {
      task.get();
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

    void setAcl() throws Exception{
      try {
        if (fs.exists(path)) {
          if (fs.isDirectory(path)) {
            aclOperation.apply(fs, path, dirAcl);
          } else {
            aclOperation.apply(fs, path, fileAcl);
          }
        }
      } catch (Exception e) {
        LOG.error("set acl error for path: " + path + ", " + e);
        throw new Exception(e);
      }
    }
  }

  class PathHelper {
    Configuration conf;
    Path rootDir;
    Path tmpDataDir;
    Path dataDir;
    Path archiveDataDir;
    Path snapshotDir;

    PathHelper(Configuration conf) {
      this.conf = conf;
      rootDir = new Path(conf.get(HConstants.HBASE_DIR));
      tmpDataDir = new Path(new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY),
          HConstants.BASE_NAMESPACE_DIR);
      dataDir = new Path(rootDir, HConstants.BASE_NAMESPACE_DIR);
      archiveDataDir = new Path(new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY),
          HConstants.BASE_NAMESPACE_DIR);
      snapshotDir = new Path(rootDir, HConstants.SNAPSHOT_DIR_NAME);
    }

    Path getRootDir() {
      return rootDir;
    }

    Path getDataDir() {
      return dataDir;
    }

    Path getTmpDataDir() {
      return tmpDataDir;
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
}

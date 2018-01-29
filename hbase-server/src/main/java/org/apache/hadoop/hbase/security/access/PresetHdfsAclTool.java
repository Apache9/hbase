package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;

public class PresetHdfsAclTool {

  private static final Log LOG = LogFactory.getLog(PresetHdfsAclTool.class);

  private static final String HBASE_ADMIN = "hbase_admin";
  private static final String HBASE_TST_ADMIN = "hbase_tst_admin";
  private static final String HBASE_NAMESPACE = "hbase";
  private static final String ACL_TABLE = "hbase:acl";
  private static final String CANARY_TABLE = "_canary_";
  // Total support 16 users(32 ACLs). 5 fixed users: owner, group, other, mask and hbase_admin.
  private static final int TABLE_USER_NUM_THRESHOLD = 10;
  private static final int NAMESPACE_USER_NUM_THRESHOLD = 5;

  private static final FsPermission PUBLIC_DIR_PERMISSION = new FsPermission((short) 0755);
  private static final FsPermission RESTORE_DIR_PERMISSION = new FsPermission((short) 0757);

  private Configuration conf;
  private HBaseAdmin admin;
  private HdfsAclManager hdfsAclManager;
  private FileSystem fs;
  private HdfsAclManager.PathHelper pathHelper;

  private Set<String> ignoreNamespaceSets = Sets.newHashSet(HBASE_NAMESPACE);
  private Set<String> ignoreTableSets = Sets.newHashSet(CANARY_TABLE);
  private Set<String> ignoreUserSets = Sets.newHashSet(HBASE_ADMIN, HBASE_TST_ADMIN);

  public PresetHdfsAclTool(Configuration conf) {
    this.conf = conf;
  }

  private void init() throws Exception {
    if (conf == null) {
      conf = HBaseConfiguration.create();
    }
    if (!conf.getBoolean(HConstants.HDFS_ACL_ENABLE, false)) {
      throw new Exception("configuration hbase.hdfs.acl.enable is false");
    }
    admin = new HBaseAdmin(conf);
    hdfsAclManager = new HdfsAclManager(conf);
    hdfsAclManager.start();
    // variables get from hdfsAclManager
    fs = hdfsAclManager.getFileSystem();
    pathHelper = hdfsAclManager.getPathHelper();
  }

  private Map<String, ArrayList<TablePermission>> loadAllPerms() throws IOException{
    Map<String, ArrayList<TablePermission>> allPermsByUsername = new TreeMap<>();
    Map<byte[], ListMultimap<String, TablePermission>> allPerms = AccessControlLists.loadAll(conf);
    // Map <user> to TablePermission
    for (Entry<byte[], ListMultimap<String, TablePermission>> entry : allPerms.entrySet()) {
      ListMultimap<String, TablePermission> perms = entry.getValue();
      for (String user : perms.keySet()) {
        allPermsByUsername.computeIfAbsent(user, userName -> new ArrayList<>())
          .addAll(perms.get(user));
      }
    }
    return allPermsByUsername;
  }

  private void cleanup() throws IOException {
    if (hdfsAclManager != null) {
      hdfsAclManager.stop();
    }
    if (admin != null) {
      admin.close();
    }
  }

  private void checkDirsAndSetPermission() throws IOException {
    Path rootDir = pathHelper.rootDir;

    checkDirAndSetPermission(rootDir);
    checkDirAndSetPermission(new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY));
    checkDirAndSetPermission(pathHelper.getArchiveDataDir());
    checkDirAndSetPermission(pathHelper.getDataDir());
    checkDirAndSetPermission(new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY));
    checkDirAndSetPermission(pathHelper.getTmpDataDir());
    checkDirAndSetPermission(pathHelper.getSnapshotRootDir());

    Path restoreDir = new Path(
      conf.get(HConstants.SNAPSHOT_RESTORE_TMP_DIR, HConstants.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT));
    checkDir(restoreDir);
    fs.setPermission(restoreDir, RESTORE_DIR_PERMISSION);
  }

  private void presetHdfsAclInternal() throws IOException {
    Map<String, ArrayList<TablePermission>> allPermsByUsername = loadAllPerms();
    for (Entry<String, ArrayList<TablePermission>> entry : allPermsByUsername.entrySet()) {
      String user = entry.getKey();
      if (ignoreUserSets.contains(user)) {
        continue;
      }
      for (final TablePermission perm : entry.getValue()) {
        UserPermission userPerm = new UserPermission(Bytes.toBytes(user), perm);
        if (isGlobalPerm(perm)) {
          LOG.info(String.format("set global user:%s acl", user));
          hdfsAclManager.grantAcl(userPerm, AccessControlProtos.Permission.Type.Global, null);
        } else if (isNamespacePerm(perm) && !ignoreNamespaceSets.contains(perm.getNamespace())) {
          String namespace = perm.getNamespace();
          LOG.info(String.format("set ns:%s, user:%s acl", namespace, user));
          // check archive/data/ns and .tmp/data/ns dir
          checkDir(pathHelper.getArchiveNsDir(namespace));
          checkDir(pathHelper.getTmpNsDir(namespace));
          hdfsAclManager.grantAcl(userPerm, AccessControlProtos.Permission.Type.Namespace, null);
        } else if (isTablePerm(perm)
            && !ignoreTableSets.contains(perm.getTableName().getNameAsString())) {
          TableName tableName = perm.getTableName();
          LOG.info(String.format("set table:%s, user:%s acl", tableName.getNameAsString(), user));
          // check archive/data/ns/table dir
          checkDir(pathHelper.getArchiveTableDir(tableName));
          hdfsAclManager.grantAcl(userPerm, AccessControlProtos.Permission.Type.Table, null);
        }
      }
    }
  }

  private void checkDirAndSetPermission(Path path) throws IOException {
    checkDir(path);
    fs.setPermission(path, PUBLIC_DIR_PERMISSION);
  }

  private void checkDir(Path path) throws IOException {
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
  }

  private boolean isGlobalPerm(TablePermission perm) {
    return isTablePerm(perm) && Bytes.equals(perm.getTableName().getName(), Bytes.toBytes(ACL_TABLE));
  }

  private boolean isNamespacePerm(TablePermission perm) {
    return perm.hasNamespace() && !perm.hasTable();
  }

  private boolean isTablePerm(TablePermission perm) {
    return perm.hasTable() && !perm.hasFamily();
  }

  private boolean isFamilyPerm(TablePermission perm) {
    return perm.hasTable() && perm.hasFamily() && !perm.hasQualifier();
  }

  private boolean isColumnPerm(TablePermission perm) {
    return perm.hasTable() && perm.hasFamily() && perm.hasQualifier();
  }

  private boolean subsetOfAction(TablePermission perm, TablePermission subPerm) {
    for (int i = 0; i < subPerm.getActions().length; i++) {
      final Action action = subPerm.getActions()[i];
      boolean found = Arrays.stream(perm.getActions()).anyMatch(a -> a == action);
      if (!found) {
        return false;
      }
    }
    return true;
  }

  private String toRevokeCommand(String user, TablePermission perm) {
    if (isGlobalPerm(perm)) {
      return String.format("revoke '%s'", user);
    }
    if (isNamespacePerm(perm)) {
      return String.format("revoke '%s', '@%s'", user, perm.getNamespace());
    }
    if (isTablePerm(perm)) {
      return String.format("revoke '%s', '%s'", user, perm.getTableName());
    }
    if (isFamilyPerm(perm)) {
      return String.format("revoke '%s', '%s', '%s'", user, perm.getTableName(),
        Bytes.toString(perm.getFamily()));
    }
    if (isColumnPerm(perm)) {
      return String.format("revoke '%s', '%s', '%s', '%s'", user, perm.getTableName(),
        Bytes.toString(perm.getFamily()), Bytes.toString(perm.getQualifier()));
    }
    return null;
  }

  private void updateDuplicateCounter(String user, TablePermission perm, TablePermission subPerm,
      AtomicInteger dupSize) {
    dupSize.incrementAndGet();
    LOG.error(String.format("%s is duplicated with %s ACL in HBase. \n%s", perm, subPerm,
      toRevokeCommand(user, subPerm)));
  }

  @VisibleForTesting
  int checkDuplicateGrantInHBaseACL() throws IOException {
    Map<String, ArrayList<TablePermission>> allPermsByUsername = loadAllPerms();
    AtomicInteger dupSize = new AtomicInteger(0);
    for (Entry<String, ArrayList<TablePermission>> entry : allPermsByUsername.entrySet()) {
      final String user = entry.getKey();
      List<TablePermission> perms = entry.getValue();
      for (int i = 0; i < perms.size(); i++) {
        final TablePermission perm = perms.get(i);
        if (isGlobalPerm(perm)) {
          LOG.warn("Found a user with global acl, please check: " + user + ", %s"
              + toRevokeCommand(user, perm));
          continue;
        }
        if (isNamespacePerm(perm)) {
          perms.stream().filter(this::isTablePerm)
              .filter(p -> perm.getNamespace().equals(p.getTableName().getNamespaceAsString()))
              .filter(p -> subsetOfAction(perm, p))
              .forEach(p -> updateDuplicateCounter(user, perm, p, dupSize));
          perms.stream().filter(this::isFamilyPerm)
              .filter(p -> perm.getNamespace().equals(p.getTableName().getNamespaceAsString()))
              .filter(p -> subsetOfAction(perm, p))
              .forEach(p -> updateDuplicateCounter(user, perm, p, dupSize));
          perms.stream().filter(this::isColumnPerm)
              .filter(p -> perm.getNamespace().equals(p.getTableName().getNamespaceAsString()))
              .filter(p -> subsetOfAction(perm, p))
              .forEach(p -> updateDuplicateCounter(user, perm, p, dupSize));
        } else if (isTablePerm(perm)) {
          perms.stream().filter(this::isFamilyPerm)
              .filter(p -> perm.getTableName().equals(p.getTableName()))
              .filter(p -> subsetOfAction(perm, p))
              .forEach(p -> updateDuplicateCounter(user, perm, p, dupSize));
          perms.stream().filter(this::isColumnPerm)
              .filter(p -> perm.getTableName().equals(p.getTableName()))
              .filter(p -> subsetOfAction(perm, p))
              .forEach(p -> updateDuplicateCounter(user, perm, p, dupSize));
        } else if (isFamilyPerm(perm)) {
          perms.stream().filter(this::isColumnPerm)
              .filter(p -> perm.getTableName().equals(p.getTableName()))
              .filter(p -> Bytes.equals(perm.getFamily(), p.getFamily()))
              .filter(p -> subsetOfAction(perm, p))
              .forEach(p -> updateDuplicateCounter(user, perm, p, dupSize));
        }
      }
    }
    return dupSize.get();
  }

  private Map<String, Integer> checkHdfsAclEntryExceededInternal() throws IOException {
    Set<String> globalUserSet = new HashSet<>();
    Map<String, Set<String>> nsUserMap = new HashMap<>();
    Map<TableName, Set<String>> tableUserMap = new HashMap<>();

    Map<String, ArrayList<TablePermission>> allPermsByUsername = loadAllPerms();
    for (Entry<String, ArrayList<TablePermission>> entry : allPermsByUsername.entrySet()) {
      String user = entry.getKey();
      if (ignoreUserSets.contains(user)) {
        continue;
      }
      for (final TablePermission perm : entry.getValue()) {
        if (isGlobalPerm(perm)) {
          globalUserSet.add(user);
        } else if (isNamespacePerm(perm) && !ignoreNamespaceSets.contains(perm.getNamespace())) {
          nsUserMap.computeIfAbsent(perm.getNamespace(), ns -> new HashSet<>()).add(user);
        } else if (isTablePerm(perm)
            && !ignoreTableSets.contains(perm.getTableName().getNameAsString())) {
          tableUserMap.computeIfAbsent(perm.getTableName(), table -> new HashSet<>()).add(user);
        }
      }
    }

    nsUserMap.entrySet().forEach(en -> {
      en.getValue().addAll(globalUserSet);
      if (en.getValue().size() > NAMESPACE_USER_NUM_THRESHOLD) {
        LOG.warn("Namespace: " + en.getKey() + " has " + en.getValue().size() + " users.");
      }
    });
    tableUserMap.entrySet().forEach(e -> {
      String ns = e.getKey().getNamespaceAsString();
      e.getValue().addAll(nsUserMap.containsKey(ns) ? nsUserMap.get(ns) : globalUserSet);
      if (e.getValue().size() > TABLE_USER_NUM_THRESHOLD) {
        LOG.error(
          "Table: " + e.getKey().getNameAsString() + " has " + e.getValue().size() + " users.");
      }
    });

    // return value used by test
    Map<String, Integer> userNumMap = new HashMap<>();
    nsUserMap.entrySet().forEach(e -> userNumMap.put(e.getKey(), e.getValue().size()));
    tableUserMap.entrySet()
        .forEach(e -> userNumMap.put(e.getKey().getNameAsString(), e.getValue().size()));
    return userNumMap;
  }

  public Map<String, Integer> checkHdfsAclEntryExceeded() {
    try {
      init();
      Map<String, Integer> result = checkHdfsAclEntryExceededInternal();
      LOG.info("Finished to check HDFS entry exceeded.");
      return result;
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      try {
        cleanup();
      } catch (IOException e) {
        LOG.error(e);
      }
    }
    return null;
  }

  public void presetHdfsAcl() {
    try {
      init();
      checkDirsAndSetPermission();
      presetHdfsAclInternal();
      LOG.info("Finished to pre-set HDFS ACL.");
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      try {
        cleanup();
      } catch (IOException e) {
        LOG.error(e);
      }
    }
  }

  public void checkDuplicateHBaseACL() {
    try {
      init();
      checkDuplicateGrantInHBaseACL();
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      try {
        cleanup();
      } catch (IOException e) {
        LOG.error(e);
      }
    }
  }

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    PresetHdfsAclTool tool = new PresetHdfsAclTool(conf);
    if (args.length == 1 && args[0].equals("checkDuplicateHBaseACL")) {
      tool.checkDuplicateHBaseACL();
    } else if (args.length == 1 && args[0].equals("presetHDFSAcl")) {
      tool.presetHdfsAcl();
    } else if (args.length == 1 && args[0].equals("checkHDFSAclExceeded")) {
      tool.checkHdfsAclEntryExceeded();
    } else {
      System.err.println("Usage: PresetHdfsAclTool [checkDuplicateHBaseACL|checkHDFSAclExceeded|presetHDFSAcl]");
      System.exit(1);
    }
  }
}

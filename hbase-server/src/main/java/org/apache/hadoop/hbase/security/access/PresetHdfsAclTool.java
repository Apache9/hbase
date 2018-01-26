package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;

public class PresetHdfsAclTool {

  private static final Log LOG = LogFactory.getLog(PresetHdfsAclTool.class);

  private static final String HBASE_ADMIN = "hbase_admin";
  private static final String HBASE_TST_ADMIN = "hbase_tst_admin";
  private static final String ACL_TABLE = "hbase:acl";
  private static final String CANARY_TABLE = "_canary_";

  private static final FsPermission PUBLIC_DIR_PERMISSION = new FsPermission((short) 0755);
  private static final FsPermission RESTORE_DIR_PERMISSION = new FsPermission((short) 0757);

  private Configuration conf;
  private HBaseAdmin admin;
  private HdfsAclManager hdfsAclManager;
  private FileSystem fs;
  private HdfsAclManager.PathHelper pathHelper;

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

  private void presetAcl() throws IOException {
    // set table acl
    HTableDescriptor[] hTableDescriptors = admin.listTables();
    for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
      TableName tableName = hTableDescriptor.getTableName();
      if (ignoreTableSets.contains(tableName.getNameAsString())) {
        continue;
      }
      List<UserPermission> userPermissions =
          AccessControlLists.getUserTablePermissions(conf, tableName);
      for (UserPermission userPermission : userPermissions) {
        String user = Bytes.toString(userPermission.getUser());
        if (ignoreUserSets.contains(user)) {
          continue;
        }
        for (Permission.Action action : userPermission.getActions()) {
          if (action == Permission.Action.READ) {
            LOG.info(String.format("set table:%s, user:%s acl", tableName.getNameAsString(), user));
            // check archive/data/ns/table dir
            checkDir(pathHelper.getArchiveTableDir(tableName));
            hdfsAclManager.grantAcl(userPermission, AccessControlProtos.Permission.Type.Table, null);
            break;
          }
        }
      }
    }

    // set namespace acl
    NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
    for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
      String namespace = namespaceDescriptor.getName();
      List<UserPermission> userPermissions =
          AccessControlLists.getUserNamespacePermissions(conf, namespace);
      for (UserPermission userPermission : userPermissions) {
        String user = Bytes.toString(userPermission.getUser());
        if (ignoreUserSets.contains(user)) {
          continue;
        }
        for (Permission.Action action : userPermission.getActions()) {
          if (action == Permission.Action.READ) {
            LOG.info(String.format("set ns:%s, user:%s acl", namespace, user));
            // check archive/data/ns and .tmp/data/ns dir
            checkDir(pathHelper.getArchiveNsDir(namespace));
            checkDir(pathHelper.getTmpNsDir(namespace));
            hdfsAclManager.grantAcl(userPermission, AccessControlProtos.Permission.Type.Namespace, null);
            break;
          }
        }
      }
    }

    // set global acl
    List<UserPermission> userPermissions =
        AccessControlLists.getUserTablePermissions(conf, TableName.valueOf(ACL_TABLE));
    for (UserPermission userPermission : userPermissions) {
      String user = Bytes.toString(userPermission.getUser());
      if (ignoreUserSets.contains(user)) {
        continue;
      }
      for (Permission.Action action : userPermission.getActions()) {
        if (action == Permission.Action.READ) {
          LOG.info(String.format("set global user:%s acl", user));
          hdfsAclManager.grantAcl(userPermission, AccessControlProtos.Permission.Type.Global, null);
          break;
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
    return !perm.hasNamespace() && !perm.hasTable();
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
    Map<byte[], ListMultimap<String, TablePermission>> allPerms = AccessControlLists.loadAll(conf);
    // Map <user> to TablePermission
    Map<String, ArrayList<TablePermission>> allPermsByUsername = new TreeMap<>();
    for (Entry<byte[], ListMultimap<String, TablePermission>> entry : allPerms.entrySet()) {
      ListMultimap<String, TablePermission> perms = entry.getValue();
      for (String user : perms.keySet()) {
        allPermsByUsername.computeIfAbsent(user, userName -> new ArrayList<>())
            .addAll(perms.get(user));
      }
    }

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

  public void execute() {
    try {
      init();
      checkDirsAndSetPermission();
      presetAcl();
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

  public void check() {
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
    if (args.length == 1 && args[0].equals("check")) {
      tool.check();
    } else if (args.length == 1 && args[0].equals("execute")) {
      tool.execute();
    } else {
      System.err.println("Usage: PresetHdfsAclTool [check|execute]");
      System.exit(1);
    }
  }
}

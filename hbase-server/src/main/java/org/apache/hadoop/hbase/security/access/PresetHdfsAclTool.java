package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.List;
import java.util.Set;

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

  public PresetHdfsAclTool(){}

  // used by TestPresetHdfsAclTool
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

  public void run() {
    try {
      init();
      checkDirsAndSetPermission();
      presetAcl();
      LOG.info("finish preset hdfs acl");
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
    PresetHdfsAclTool tool = new PresetHdfsAclTool();
    tool.run();
  }
}

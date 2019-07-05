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
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;

public class PresetHdfsAclTool {

  private static final Log LOG = LogFactory.getLog(PresetHdfsAclTool.class);

  private static final String HBASE_ADMIN = "hbase_admin";
  private static final String HBASE_TST_ADMIN = "hbase_tst_admin";
  private static final String HBASE_NAMESPACE = "hbase";
  private static final String ACL_TABLE = "hbase:acl";
  private static final String CANARY_TABLE = "_canary_";
  // Total support 16 users(32 ACLs). 5 fixed users: owner, group, other, mask and hbase_admin.
  private static final int TABLE_USER_NUM_THRESHOLD = 11;
  private static final int NAMESPACE_USER_NUM_THRESHOLD = 5;
  private static final String PRESET_INCLUDE_PARAM = "include";

  private Configuration conf;
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

  private void cleanup() {
    if (hdfsAclManager != null) {
      hdfsAclManager.stop();
    }
  }

  private void presetHdfsAclInternal(String[] args) throws IOException {
    if (args != null && args.length >= 2) {
      String option = args[1];
      if (option != null && option.equals(PRESET_INCLUDE_PARAM)) {
        Set<String> tableSet = new HashSet<>();
        for (int i = 2; i < args.length; i++) {
          tableSet.add(args[i]);
        }
        presetIncludeHdfsAcl(tableSet);
      } else {
        System.err.println(USAGE_MESSAGE);
        return;
      }
    } else {
      presetAllHdfsAcl();
    }
  }

  private void presetIncludeHdfsAcl(Set<String> tables) throws IOException {
    for (Entry<String, List<String>> tableUserEntry : getTableReadPermUsers(tables).entrySet()) {
      String table = tableUserEntry.getKey();
      checkDir(pathHelper.getArchiveTableDir(TableName.valueOf(table)));
      tableUserEntry.getValue().forEach(user -> {
        LOG.info(String.format("Set table: %s, user: %s acl.", table, user));
        UserPermission userPermission = new UserPermission(Bytes.toBytes(user),
                new TablePermission(TableName.valueOf(table), null, Action.READ));
        hdfsAclManager.grantAcl(userPermission, AccessControlProtos.Permission.Type.Table, null);
      });
    }
  }

  private Map<String, List<String>> getTableReadPermUsers(Set<String> tables) throws IOException {
    Map<byte[], ListMultimap<String, TablePermission>> allPerms = AccessControlLists.loadAll(conf);
    Map<String, List<String>> tablePermissions = new HashMap<>();
    for (String table : tables) {
      List<Entry<String, TablePermission>> permissions = new ArrayList<>();
      permissions.addAll(allPerms.getOrDefault(Bytes.toBytes(ACL_TABLE), ArrayListMultimap.create()).entries());
      permissions.addAll(allPerms.getOrDefault(
              Bytes.toBytes(AccessControlLists.GROUP_PREFIX + TableName.valueOf(table).getNamespaceAsString()),
              ArrayListMultimap.create()).entries());
      permissions.addAll(allPerms.getOrDefault(Bytes.toBytes(table), ArrayListMultimap.create()).entries().stream()
              .filter(e -> isTablePerm(e.getValue())).collect(Collectors.toList()));
      tablePermissions.put(table, permissions.stream()
              .filter(p -> !ignoreUserSets.contains(p.getKey()) && Arrays.stream(p.getValue().actions).anyMatch(a -> a == Action.READ))
              .map(p -> p.getKey()).collect(Collectors.toList()));
    }
    return tablePermissions;
  }

  private void presetAllHdfsAcl() throws IOException {
    for (Entry<String, ArrayList<TablePermission>> entry : loadAllPerms().entrySet()) {
      String user = entry.getKey();
      if (ignoreUserSets.contains(user)) {
        continue;
      }
      for (final TablePermission perm : entry.getValue()) {
        UserPermission userPerm = new UserPermission(Bytes.toBytes(user), perm);
        if (isGlobalPerm(perm)) {
          LOG.info(String.format("Set global user: %s acl.", user));
          hdfsAclManager.grantAcl(userPerm, AccessControlProtos.Permission.Type.Global, null);
        } else if (isNamespacePerm(perm)) {
          String namespace = perm.getNamespace();
          if (!ignoreNamespaceSets.contains(namespace)) {
            LOG.info(String.format("Set ns: %s, user: %s acl.", namespace, user));
            // check archive/data/ns and .tmp/data/ns dir
            checkDir(pathHelper.getArchiveNsDir(namespace));
            checkDir(pathHelper.getTmpNsDir(namespace));
            hdfsAclManager.grantAcl(userPerm, AccessControlProtos.Permission.Type.Namespace, null);
          }
        } else if (isTablePerm(perm)) {
          TableName tableName = perm.getTableName();
          String table = tableName.getNameAsString();
          if (!ignoreTableSets.contains(table)) {
            LOG.info(String.format("Set table: %s, user: %s acl.", table, user));
            // check archive/data/ns/table dir
            checkDir(pathHelper.getArchiveTableDir(tableName));
            hdfsAclManager.grantAcl(userPerm, AccessControlProtos.Permission.Type.Table, null);
          }
        }
      }
    }
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
          LOG.warn("Found a user with global acl, please check: " + user + ", "
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
      cleanup();
    }
    return null;
  }

  public void presetHdfsAcl(String[] args) {
    try {
      init();
      HdfsAclManager.setPublicDirectoryPermission(conf, fs);
      HdfsAclManager.setRestoreDirectoryPermission(conf, fs);
      presetHdfsAclInternal(args);
      LOG.info("Finished to pre-set HDFS ACL.");
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      cleanup();
    }
  }

  public void checkDuplicateHBaseACL() {
    try {
      init();
      checkDuplicateGrantInHBaseACL();
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      cleanup();
    }
  }

  private void fixTmpDirectoryAcls() {
    try {
      init();
      fixTmpDirectoryAcls(conf, fs);
    } catch (Exception e) {
      LOG.error("Fix tmp directory acl error", e);
    }
  }

  /**
   * Fix tmp directory default and access acls, because master deletes this dir when start and lose
   * these acls.
   */
  @VisibleForTesting
  void fixTmpDirectoryAcls(Configuration conf, FileSystem fs) throws IOException {
    if (conf.getBoolean(HConstants.HDFS_ACL_ENABLE, false)) {
      HdfsAclManager.PathHelper pathHelper = new HdfsAclManager.PathHelper(conf);
      for (Map.Entry<byte[], ListMultimap<String, TablePermission>> entry : AccessControlLists
          .loadAll(conf).entrySet()) {
        byte[] permissionEntry = entry.getKey();
        Set<String> users = getUsersWithReadPermission(entry.getValue());
        List<AclEntry> defaultAndAccessAclEntries = getDefaultAndAccessAclEntries(users);
        if (AccessControlLists.isGlobalEntry(permissionEntry)) {
          Path tmpDataDir = pathHelper.getTmpDataDir();
          setAclRecursive(fs, tmpDataDir, defaultAndAccessAclEntries);
        } else if (AccessControlLists.isNamespaceEntry(permissionEntry)) {
          String namespace = Bytes.toString(AccessControlLists.fromNamespaceEntry(permissionEntry));
          Path tmpNsDir = pathHelper.getTmpNsDir(namespace);
          setAclRecursive(fs, tmpNsDir, defaultAndAccessAclEntries);
        } else {
          TableName tableName = TableName.valueOf(permissionEntry);
          Path tmpTableDir = pathHelper.getTmpTableDir(tableName);
          setAclRecursive(fs, tmpTableDir, defaultAndAccessAclEntries);
          List<AclEntry> accessAclEntries = getAccessAclEntries(users);
          fs.modifyAclEntries(tmpTableDir.getParent(), accessAclEntries);
        }
      }
      LOG.info("Finish fix tmp directories acls used for scanning snapshot.");
    }
  }

  private void setAclRecursive(FileSystem fs, Path path, List<AclEntry> aclEntries)
      throws IOException {
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }
    fs.modifyAclEntries(path, aclEntries);
    FileStatus[] fileStatuses = fs.listStatus(path);
    for (FileStatus fileStatus : fileStatuses) {
      setAclRecursive(fs, fileStatus.getPath(), aclEntries);
    }
  }

  private Set<String>
      getUsersWithReadPermission(ListMultimap<String, TablePermission> permissions) {
    Set<String> users = new HashSet<>();
    for (Map.Entry<String, TablePermission> entry : permissions.entries()) {
      TablePermission tablePermission = entry.getValue();
      if (!tablePermission.hasFamily() && !tablePermission.hasQualifier()
          && tablePermission.implies(Permission.Action.READ)) {
        users.add(entry.getKey());
      }
    }
    return users;
  }

  private List<AclEntry> getDefaultAndAccessAclEntries(Set<String> users) {
    List<AclEntry> aclEntries = new ArrayList<>();
    for (String user : users) {
      aclEntries.add(aclEntry(ACCESS, user));
      aclEntries.add(aclEntry(DEFAULT, user));
    }
    return aclEntries;
  }

  private List<AclEntry> getAccessAclEntries(Set<String> users) {
    List<AclEntry> aclEntries = new ArrayList<>();
    for (String user : users) {
      aclEntries.add(aclEntry(ACCESS, user));
    }
    return aclEntries;
  }

  private AclEntry aclEntry(AclEntryScope scope, String name) {
    return new AclEntry.Builder().setScope(scope).setType(USER).setName(name)
        .setPermission(READ_EXECUTE).build();
  }

  private static String formatMsg(String key, String val) {
    return String.format("  %-38s: %s\n", key, val);
  }

  private static final String USAGE_MESSAGE =
      new StringBuilder("Usage: PresetHdfsAclTool [option] [arg]\n")
          .append("Options and arguments:\n")
          .append(formatMsg("checkDuplicateHBaseACL", "check duplicate acls granted in HBase"))
          .append(formatMsg("checkHDFSAclExceeded", "check tables which has more than 12 users"))
          .append(formatMsg("presetHDFSAcl",
            "preset hdfs acl for all granted hbase acls(global namespace and table)"))
          .append(formatMsg("presetHDFSAcl include [tableName ...]",
            "preset hdfs acl just for the specified tables"))
          .append(formatMsg("fixTmpDirACL", "fix tmp directory acls"))
          .toString();

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    PresetHdfsAclTool tool = new PresetHdfsAclTool(conf);
    if (args.length == 1 && args[0].equals("checkDuplicateHBaseACL")) {
      tool.checkDuplicateHBaseACL();
    } else if (args.length >= 1 && args[0].equals("presetHDFSAcl")) {
      tool.presetHdfsAcl(args);
    } else if (args.length == 1 && args[0].equals("checkHDFSAclExceeded")) {
      tool.checkHdfsAclEntryExceeded();
    } else if (args.length == 1 && args[0].equals("fixTmpDirACL")) {
      tool.fixTmpDirectoryAcls();
    } else {
      System.err.println(USAGE_MESSAGE);
      System.exit(1);
    }
  }
}

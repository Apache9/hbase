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
package org.apache.hadoop.hbase.rsgroup;

import com.google.protobuf.Service;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

// TODO: Encapsulate MasterObserver functions into separate subclass.
@CoreCoprocessor
@InterfaceAudience.Private
public class RSGroupAdminEndpoint implements MasterCoprocessor, MasterObserver {
  // Only instance of RSGroupInfoManager. RSGroup aware load balancers ask for this instance on
  // their setup.
  private MasterServices master;
  private RSGroupInfoManager groupInfoManager;
  private RSGroupAdminServer groupAdminServer;
  private RSGroupAdminServiceImpl groupAdminService = new RSGroupAdminServiceImpl();

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof HasMasterServices)) {
      throw new IOException("Does not implement HMasterServices");
    }

    master = ((HasMasterServices) env).getMasterServices();
    groupInfoManager = RSGroupInfoManagerImpl.getInstance(master);
    groupAdminServer = new RSGroupAdminServer(master, groupInfoManager);
    Class<?> clazz =
      master.getConfiguration().getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, null);
    if (!RSGroupableBalancer.class.isAssignableFrom(clazz)) {
      throw new IOException("Configured balancer does not support RegionServer groups.");
    }
    AccessChecker accessChecker = ((HasMasterServices) env).getMasterServices().getAccessChecker();

    // set the user-provider.
    UserProvider userProvider = UserProvider.instantiate(env.getConfiguration());
    groupAdminService.initialize(master, groupAdminServer, accessChecker, userProvider);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(groupAdminService);
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  RSGroupInfoManager getGroupInfoManager() {
    return groupInfoManager;
  }

  @VisibleForTesting
  RSGroupAdminServiceImpl getGroupAdminService() {
    return groupAdminService;
  }

  /////////////////////////////////////////////////////////////////////////////
  // MasterObserver overrides
  /////////////////////////////////////////////////////////////////////////////

  @Override
  public void postClearDeadServers(ObserverContext<MasterCoprocessorEnvironment> ctx,
    List<ServerName> servers, List<ServerName> notClearedServers) throws IOException {
    Set<Address> clearedServer =
      servers.stream().filter(server -> !notClearedServers.contains(server))
        .map(ServerName::getAddress).collect(Collectors.toSet());
    if (!clearedServer.isEmpty()) {
      groupAdminServer.removeServers(clearedServer);
    }
  }

  private RSGroupInfo checkGroupExists(Optional<String> optGroupName) throws IOException {
    if (optGroupName.isPresent()) {
      String groupName = optGroupName.get();
      RSGroupInfo group = groupAdminServer.getRSGroupInfo(groupName);
      if (group == null) {
        throw new ConstraintException("Region server group " + groupName + " does not exit");
      }
      return group;
    }
    return null;
  }

  // Do not allow creating new tables which has an empty rs group, expect the default rs group.
  // Notice that we do not check for online servers, as this is not stable because region server can
  // die at any time.
  private void checkForEmptyGroup(TableDescriptor desc) throws IOException {
    if (desc.getTableName().isSystemTable()) {
      // do not check for system tables as we may block the bootstrap.
      return;
    }
    RSGroupInfo rsGroupInfo;
    Optional<String> optGroupName = desc.getRegionServerGroup();
    if (optGroupName.isPresent()) {
      String groupName = optGroupName.get();
      if (groupName.equals(RSGroupInfo.DEFAULT_GROUP)) {
        // do not check for default group
        return;
      }
      rsGroupInfo = groupAdminServer.getRSGroupInfo(groupName);
      if (rsGroupInfo == null) {
        throw new ConstraintException(
          "RSGroup " + groupName + " for table " + desc.getTableName() + " does not exist");
      }
    } else {
      NamespaceDescriptor nd =
        master.getClusterSchema().getNamespace(desc.getTableName().getNamespaceAsString());
      String groupNameOfNs = nd.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP);
      if (groupNameOfNs == null || groupNameOfNs.equals(RSGroupInfo.DEFAULT_GROUP)) {
        // do not check for default group
        return;
      }
      rsGroupInfo = groupAdminServer.getRSGroupInfo(groupNameOfNs);
      if (rsGroupInfo == null) {
        throw new ConstraintException("RSGroup " + groupNameOfNs + " for table " +
          desc.getTableName() + "(inherit from namespace) does not exist");
      }
    }
    if (rsGroupInfo.getServers().isEmpty()) {
      throw new ConstraintException(
        "No servers in the rsgroup " + rsGroupInfo.getName() + " for " + desc);
    }
  }

  @Override
  public void preCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableDescriptor desc, RegionInfo[] regions) throws IOException {
    checkGroupExists(desc.getRegionServerGroup());
    checkForEmptyGroup(desc);
  }

  @Override
  public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName, TableDescriptor currentDescriptor, TableDescriptor newDescriptor)
    throws IOException {
    if (!currentDescriptor.getRegionServerGroup().equals(newDescriptor.getRegionServerGroup())) {
      RSGroupInfo group = checkGroupExists(newDescriptor.getRegionServerGroup());
      if (group != null && group.getServers().isEmpty()) {
        throw new ConstraintException(
          "No servers in the rsgroup " + group.getName() + " for " + newDescriptor);
      }
    }
    return MasterObserver.super.preModifyTable(ctx, tableName, currentDescriptor, newDescriptor);
  }

  private void checkNamespaceGroup(NamespaceDescriptor ns) throws IOException {
    String groupName = ns.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP);
    if (groupName == null) {
      return;
    }
    RSGroupInfo group = groupAdminServer.getRSGroupInfo(groupName);
    if (group == null) {
      throw new ConstraintException(
        "RSGroup " + groupName + " for namespace " + ns.getName() + " does not exist");
    }
    if (group.getServers().isEmpty()) {
      throw new ConstraintException(
        "No servers in the rsgroup " + group.getName() + " for namespace " + ns.getName());
    }
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor ns) throws IOException {
    checkNamespaceGroup(ns);
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    NamespaceDescriptor currentNsDescriptor, NamespaceDescriptor newNsDescriptor)
    throws IOException {
    if (!Objects.equals(
      currentNsDescriptor.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP),
      newNsDescriptor.getConfigurationValue(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP))) {
      checkNamespaceGroup(newNsDescriptor);
    }
  }
}

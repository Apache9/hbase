#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

include Java

java_import org.apache.hadoop.hbase.TableName
java_import org.apache.hadoop.hbase.replication.ReplicationPeerConfig
java_import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos::ReplicationState::State
java_import org.apache.hadoop.hbase.replication.ReplicationPeer::PeerProtocol
java_import org.apache.hadoop.hbase.client.replication::TableCFsHelper
# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class RepAdmin
    include HBaseConstants

    def initialize(configuration, formatter)
      @replication_admin = org.apache.hadoop.hbase.client.replication.ReplicationAdmin.new(configuration)
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    # Add a new peer cluster to replicate to
    def add_peer(id, cluster_key, peer_state = nil, peer_tableCFs = nil, protocol = nil)
      replication_peer_config = ReplicationPeerConfig.new
      replication_peer_config.set_cluster_key(cluster_key)
      unless peer_state.nil?
        replication_peer_config.set_state(State.valueOf(peer_state))
      end

      map = nil
      unless peer_tableCFs.nil?
        # convert table_cfs to TableName
        map = java.util.HashMap.new
        peer_tableCFs.each{|key, val|
          map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
        }
      end

      unless protocol.nil?
        replication_peer_config.set_protocol(PeerProtocol.valueOf(protocol))
      end

      @replication_admin.add_peer(id, replication_peer_config, map)
    end

    #----------------------------------------------------------------------------------------------
    # Remove a peer cluster, stops the replication
    def remove_peer(id)
      @replication_admin.removePeer(id)
    end


    #---------------------------------------------------------------------------------------------
    # Show replcated tables/column families, and their ReplicationType
    def list_replicated_tables(regex = ".*")
      pattern = java.util.regex.Pattern.compile(regex)
      list = @replication_admin.listReplicated()
      list.select {|s| pattern.match(s.get(org.apache.hadoop.hbase.client.replication.ReplicationAdmin::TNAME))}
    end

    #----------------------------------------------------------------------------------------------
    # List all peer clusters
    def list_peers
      @replication_admin.listPeerConfigs
    end

    #----------------------------------------------------------------------------------------------
    # Get peer cluster state
    def get_peer_state(id)
      @replication_admin.getPeerState(id) ? "ENABLED" : "DISABLED"
    end

    #----------------------------------------------------------------------------------------------
    # Restart the replication stream to the specified peer
    def enable_peer(id)
      @replication_admin.enablePeer(id)
    end

    #----------------------------------------------------------------------------------------------
    # Stop the replication stream to the specified peer
    def disable_peer(id)
      @replication_admin.disablePeer(id)
    end

    #----------------------------------------------------------------------------------------------
    # Show the current tableCFs config for the specified peer
    def show_peer_tableCFs(id)
      TableCFsHelper.convert(TableCFsHelper.convert(@replication_admin.getPeerTableCFs(id)))
    end

    #----------------------------------------------------------------------------------------------
    # Set new tableCFs config for the specified peer
    def set_peer_tableCFs(id, tableCFs)
      unless tableCFs.nil?
        # convert tableCFs to TableName
        map = java.util.HashMap.new
        tableCFs.each{|key, val|
          map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
        }
      end
      @replication_admin.setPeerTableCFs(id, map)
    end

    #----------------------------------------------------------------------------------------------
    # Append a tableCFs config for the specified peer
    def append_peer_tableCFs(id, tableCFs)
      unless tableCFs.nil?
        # convert tableCFs to TableName
        map = java.util.HashMap.new
        tableCFs.each{|key, val|
          map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
        }
      end
      @replication_admin.appendPeerTableCFs(id, map)
    end

    #----------------------------------------------------------------------------------------------
    # Enables a table's replication switch
    def enable_tablerep(table_name)
      tableName = TableName.valueOf(table_name)
      @replication_admin.enableTableRep(tableName)
    end

#----------------------------------------------------------------------------------------------
    # Disables a table's replication switch
    def disable_tablerep(table_name)
      tableName = TableName.valueOf(table_name)
      @replication_admin.disableTableRep(tableName)
    end

    #----------------------------------------------------------------------------------------------
    # Upgrade tableCFs 
    def upgrade_tablecfs 
      @replication_admin.upgradeTableCFs
    end
  end
end

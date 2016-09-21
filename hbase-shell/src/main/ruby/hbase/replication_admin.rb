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
java_import org.apache.hadoop.hbase.client.replication::ReplicationSerDeHelper
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
    def add_peer(id, cluster_key, args = {})
      if args.is_a?(Hash)
        replication_peer_config = ReplicationPeerConfig.new
        replication_peer_config.set_cluster_key(cluster_key)

        peer_state = args.fetch(STATE, nil)
        unless peer_state.nil?
          replication_peer_config.set_state(State.valueOf(peer_state))
        end

        namespaces = args.fetch(NAMESPACES, nil)
        unless namespaces.nil?
          ns_set = java.util.HashSet.new
          namespaces.each do |n|
            ns_set.add(n)
          end
          replication_peer_config.set_namespaces(ns_set)
        end

        table_cfs = args.fetch(TABLE_CFS, nil)
        unless table_cfs.nil?
          # convert table_cfs to TableName
          map = java.util.HashMap.new
          table_cfs.each{|key, val|
            map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
          }
          replication_peer_config.set_table_cfs_map(map)
        end

        protocol = args.fetch(PROTOCOL, nil)
        unless protocol.nil?
          replication_peer_config.set_protocol(PeerProtocol.valueOf(protocol))
        end

        @replication_admin.add_peer(id, replication_peer_config)
      else
        raise(ArgumentError, "args must be a Hash")
      end
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
      ReplicationSerDeHelper.convertToString(@replication_admin.getPeerTableCFs(id))
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
    # Remove some tableCFs from the tableCFs config of the specified peer
    def remove_peer_tableCFs(id, tableCFs)
      unless tableCFs.nil?
        # convert tableCFs to TableName
        map = java.util.HashMap.new
        tableCFs.each{|key, val|
          map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
        }
      end
      @replication_admin.removePeerTableCFs(id, map)
    end

    # Set new namespaces config for the specified peer
    def set_peer_namespaces(id, namespaces)
      unless namespaces.nil?
        ns_set = java.util.HashSet.new
        namespaces.each do |n|
          ns_set.add(n)
        end
        rpc = @replication_admin.getPeerConfig(id)
        unless rpc.nil?
          rpc.setNamespaces(ns_set)
          @replication_admin.updatePeerConfig(id, rpc)
        end
      end
    end

    # Show the current namespaces config for the specified peer
    def show_peer_namespaces(peer_config)
      namespaces = peer_config.get_namespaces
      if !namespaces.nil?
        return namespaces.join(';')
      else
        return nil
      end
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

    #----------------------------------------------------------------------------------------------
    # Set per node bandwidth for the specified peer 
    def set_peer_bandwidth(id, bandwidth)
      @replication_admin.setPeerBandwidth(id, bandwidth)
    end

    def peer_added(id)
      @replication_admin.peer_added(id)
    end

    def update_peer_config(id, args={})
      # Optional parameters
      config = args.fetch(CONFIG, nil)
      data = args.fetch(DATA, nil)

      # Create and populate a ReplicationPeerConfig
      replication_peer_config = ReplicationPeerConfig.new
      unless config.nil?
        replication_peer_config.get_configuration.put_all(config)
      end

      unless data.nil?
        # Convert Strings to Bytes for peer_data
        peer_data = replication_peer_config.get_peer_data
        data.each{|key, val|
          peer_data.put(Bytes.to_bytes(key), Bytes.to_bytes(val))
        }
      end

      @replication_admin.update_peer_config(id, replication_peer_config)
    end
  end
end

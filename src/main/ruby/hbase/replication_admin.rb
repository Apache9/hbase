#
# Copyright 2010 The Apache Software Foundation
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
    def add_peer(id, cluster_key, peer_state = nil, peer_tableCFs = nil, peer_bandwith = nil)
      @replication_admin.addPeer(id, cluster_key, peer_state, peer_tableCFs, peer_bandwith)
    end

    #----------------------------------------------------------------------------------------------
    # Remove a peer cluster, stops the replication
    def remove_peer(id)
      @replication_admin.removePeer(id)
    end

    #---------------------------------------------------------------------------------------------
    # Show replcated tables/column families, and their ReplicationType
    def list_replicated_tables
       @replication_admin.listReplicated()
    end

    #----------------------------------------------------------------------------------------------
    # List all peer clusters
    def list_peers
      @replication_admin.listPeers
    end

    #----------------------------------------------------------------------------------------------
    # Get peer cluster state
    def get_peer_state(id)
      @replication_admin.getPeerState(id)
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
      @replication_admin.getPeerTableCFs(id)
    end

    #----------------------------------------------------------------------------------------------
    # Show the current bandwidth config for the specified peer
    def show_peer_bandwidth(id)
      @replication_admin.getPeerBandwidth(id)
    end

    #----------------------------------------------------------------------------------------------
    # Set new tableCFs config for the specified peer
    def set_peer_tableCFs(id, tableCFs)
      @replication_admin.setPeerTableCFs(id, tableCFs)
    end

    # Set new bandwidth config for the specified peer
    def set_peer_bandwidth(id, bandwidth)
      @replication_admin.setPeerBandwidth(id, bandwidth)
    end

    #----------------------------------------------------------------------------------------------
    # Append a tableCFs config for the specified peer
    def append_peer_tableCFs(id, tableCFs)
      @replication_admin.appendPeerTableCFs(id, tableCFs)
    end
    
    #----------------------------------------------------------------------------------------------
    # Restart the replication, in an unknown state
    def start_replication
      @replication_admin.setReplicating(true)
    end

    #----------------------------------------------------------------------------------------------
    # Kill switch for replication, stops all its features
    def stop_replication
      @replication_admin.setReplicating(false)
    end
  end
end

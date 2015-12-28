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

module Shell
  module Commands
    class AddPeer< Command
      def help
        return <<-EOF
Add a peer cluster to replicate to, the id must be a short and
the cluster key is composed like this:
hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent
This gives a full path for HBase to connect to another cluster.
Examples:

  hbase> add_peer '1', "server1.cie.com:2181:/hbase"
  hbase> add_peer '2', "zk1,zk2,zk3:2182:/hbase-prod"
  hbase> add_peer '3', "zk4,zk5,zk6:11000:/hbase-test", "ENABLED"
  hbase> add_peer '4', "zk7,zk8,zk9:11000:/hbase-proc", "DISABLED"
  hbase> add_peer '5', "zk4,zk5,zk6:11000:/hbase-test", "ENABLED",
    { "table1" => [], "ns2:table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
  hbase> add_peer '6', "zk4,zk5,zk6:11000:/hbase-test", "ENABLED", " ", "THRIFT"
  hbase> add_peer '7', "zk4,zk5,zk6:11000:/hbase-test",
    { "table1" => [], "ns2:table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }, "THRIFT"
EOF
      end

      def command(id, cluster_key, peer_state = nil, peer_tableCFs = nil, protocol = nil)
        format_simple_command do
          replication_admin.add_peer(id, cluster_key, peer_state, peer_tableCFs, protocol)
        end
      end
    end
  end
end

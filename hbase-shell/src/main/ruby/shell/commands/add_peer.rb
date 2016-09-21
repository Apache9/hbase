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

An optional parameter for namespaces identifies which namespace's tables will be replicated
to the peer cluster.
An optional parameter for table column families identifies which tables and/or column families
will be replicated to the peer cluster.

Notice: Set a namespace in the peer config means that all tables in this namespace
will be replicated to the peer cluster. So if you already have set a namespace in peer config,
then you can't set this namespace's tables in the peer config again.

Examples:

  hbase> add_peer '1', "server1.cie.com:2181:/hbase"
  hbase> add_peer '2', "zk1,zk2,zk3:2182:/hbase-prod"
  hbase> add_peer '3', "zk4,zk5,zk6:11000:/hbase-test", STATE => "ENABLED"
  hbase> add_peer '4', "zk7,zk8,zk9:11000:/hbase-proc", STATE => "DISABLED"
  hbase> add_peer '5', "zk4,zk5,zk6:11000:/hbase-test", STATE => "ENABLED",
    NAMESPACES => ["ns1", "ns2", "ns3"]
  hbase> add_peer '6', "zk4,zk5,zk6:11000:/hbase-test", STATE => "ENABLED",
    NAMESPACES => ["ns1", "ns2"], TABLE_CFS => { "ns3:table1" => [], "ns3:table2" => ["cf1"] }
  hbase> add_peer '7', "zk4,zk5,zk6:11000:/hbase-test", STATE => "ENABLED",
    TABLE_CFS => { "ns3:table1" => [], "ns3:table2" => ["cf1"] }, PROTOCOL => "THRIFT"

  # Xiaomi HBase Cluster
  hbase> add_peer '1', "hbase://c3tst-pressure98", STATE => "ENABLED"
  hbase> add_peer '2', 'hbase://c3tst-pressure98', STATE => "ENABLED",
    NAMESPACES => ["ns1", "ns2"]
  hbase> add_peer '3', 'hbase://c3tst-pressure98', STATE => "ENABLED", NAMESPACES => ["ns1"],
    TABLE_CFS => { "ns2:table1" => [], "ns2:table2" => ["cf1"] }
  hbase> add_peer '4', 'hbase://c3tst-pressure98', STATE => "ENABLED", NAMESPACES => ["ns1"],
    TABLE_CFS => { "ns2:table1" => [], "ns2:table2" => ["cf1"] }, PROTOCOL => "THRIFT"
EOF
      end

      def command(id, cluster_key, args = {})
        format_simple_command do
          replication_admin.add_peer(id, cluster_key, args)
        end
      end
    end
  end
end

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

require 'shell'
require 'hbase/hbase'
require 'hbase/table'

include HBaseConstants

module Hbase
  class ReplicationAdminTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      @peer_id = '1'

      setup_hbase

      assert_equal(0, replication_admin.list_peers().length)
    end

    def teardown
      assert_equal(0, replication_admin.list_peers().length)
    end

    define_test "add_peer: should fail when args isn't specified" do
      assert_raise(ArgumentError) do
        replication_admin.add_peer(@peer_id, nil)
      end
    end

    define_test "add_peer: fail when neither CLUSTER_KEY nor ENDPOINT_CLASSNAME are specified" do
      assert_raise(ArgumentError) do
        args = {}
        replication_admin.add_peer(@peer_id, args)
      end
    end

    define_test "add_peer: args must be a hash" do
      assert_raise(ArgumentError) do
        replication_admin.add_peer(@peer_id, 1)
      end
      assert_raise(ArgumentError) do
        replication_admin.add_peer(@peer_id, ['test'])
      end
      assert_raise(ArgumentError) do
        replication_admin.add_peer(@peer_id, 'test')
      end
    end

    define_test "add_peer: single zk cluster key" do
      cluster_key = "server1.cie.com:2181:/hbase"

      replication_admin.add_peer(@peer_id, {CLUSTER_KEY => cluster_key})

      assert_equal(1, replication_admin.list_peers().length)
      assert_equal(cluster_key, replication_admin.list_peers().fetch(@peer_id).getClusterKey)

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key" do
      cluster_key = "zk1,zk2,zk3:2182:/hbase-prod"

      replication_admin.add_peer(@peer_id, {CLUSTER_KEY => cluster_key})

      assert_equal(1, replication_admin.list_peers().length)
      assert_equal(cluster_key, replication_admin.list_peers().fetch(@peer_id).getClusterKey)
      assert_equal(true, replication_admin.list_peers().fetch(@peer_id).replicateAllUserTables)

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: single zk cluster key - peer config" do
      cluster_key = "server1.cie.com:2181:/hbase"

      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      assert_equal(cluster_key, replication_admin.list_peers().fetch(@peer_id).getClusterKey)
      assert_equal(true, replication_admin.list_peers().fetch(@peer_id).replicateAllUserTables)

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key - peer config" do
      cluster_key = "zk1,zk2,zk3:2182:/hbase-prod"

      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      assert_equal(cluster_key, replication_admin.list_peers().fetch(@peer_id).getClusterKey)
      assert_equal(true, replication_admin.list_peers().fetch(@peer_id).replicateAllUserTables)

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key and namespaces" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2", "ns3"]
      namespaces_str = "ns1;ns2;ns3"

      args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      assert_equal(false, peer_config.replicateAllUserTables)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key and exclude namespaces" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2", "ns3"]
      namespaces_str = "!ns1;ns2;ns3"

      args = { CLUSTER_KEY => cluster_key, EXCLUDE_NAMESPACES => namespaces }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      assert_equal(true, peer_config.replicateAllUserTables)
      assert_equal(namespaces_str,
        replication_admin.show_peer_exclude_namespaces(peer_config))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key and namespaces, table_cfs" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      table_cfs = { "ns3:table1" => [], "ns3:table2" => ["cf1"],
        "ns3:table3" => ["cf1", "cf2"] }
      namespaces_str = "ns1;ns2"

      args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces,
        TABLE_CFS => table_cfs }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      assert_equal(false, replication_admin.list_peers().fetch(@peer_id).replicateAllUserTables)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))
      assert_tablecfs_equal(table_cfs, peer_config.getTableCFsMap())

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key and exclude namespaces, exclude table_cfs" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      table_cfs = { "ns3:table1" => [], "ns3:table2" => ["cf1"],
        "ns3:table3" => ["cf1", "cf2"] }
      namespaces_str = "!ns1;ns2"

      args = { CLUSTER_KEY => cluster_key, EXCLUDE_NAMESPACES => namespaces,
        EXCLUDE_TABLE_CFS => table_cfs }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      assert_equal(true, peer_config.replicateAllUserTables)
      assert_equal(namespaces_str,
        replication_admin.show_peer_exclude_namespaces(peer_config))
      assert_tablecfs_equal(table_cfs, peer_config.getExcludeTableCFsMap())

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: namespaces, table-cfs and exlude namespaces, table-cfs" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      table_cfs = { "ns3:table1" => [] }

      assert_raise(ArgumentError) do
        args = { CLUSTER_KEY => cluster_key, EXCLUDE_NAMESPACES => namespaces, TABLE_CFS => table_cfs }
        replication_admin.add_peer(@peer_id, args)
      end
      assert_raise(ArgumentError) do
        args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces, EXCLUDE_TABLE_CFS => table_cfs }
        replication_admin.add_peer(@peer_id, args)
      end
      assert_raise(ArgumentError) do
        args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces, EXCLUDE_NAMESPACES => namespaces }
        replication_admin.add_peer(@peer_id, args)
      end
      assert_raise(ArgumentError) do
        args = { CLUSTER_KEY => cluster_key, TABLE_CFS => table_cfs, EXCLUDE_TABLE_CFS => table_cfs }
        replication_admin.add_peer(@peer_id, args)
      end
    end

    def assert_tablecfs_equal(table_cfs, table_cfs_map)
      assert_equal(table_cfs.length, table_cfs_map.length)
      table_cfs_map.each{|key, value|
        assert(table_cfs.has_key?(key.getNameAsString))
        if table_cfs.fetch(key.getNameAsString).length == 0
          assert_equal(nil, value)
        else
          assert_equal(table_cfs.fetch(key.getNameAsString).length, value.length)
          value.each{|v|
            assert(table_cfs.fetch(key.getNameAsString).include?(v))
          }
        end
      }
    end

    define_test "add_peer: multiple zk cluster key and table_cfs - peer config" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      table_cfs = { "table1" => [], "table2" => ["cf1"], "table3" => ["cf1", "cf2"] }

      args = { CLUSTER_KEY => cluster_key, TABLE_CFS => table_cfs }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      assert_equal(cluster_key, replication_admin.list_peers().fetch(@peer_id).getClusterKey)
      assert_equal(false, replication_admin.list_peers().fetch(@peer_id).replicateAllUserTables)
      assert_tablecfs_equal(table_cfs, replication_admin.get_peer_config(@peer_id).getTableCFsMap())

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "set_peer_tableCFs: works with table-cfs map" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      args = { CLUSTER_KEY => cluster_key}
      replication_admin.add_peer(@peer_id, args)

      replication_admin.set_peer_replicate_all(@peer_id, false)

      assert_equal(1, replication_admin.list_peers().length)
      assert_equal(cluster_key, replication_admin.list_peers().fetch(@peer_id).getClusterKey)

      table_cfs = { "table1" => [], "table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
      replication_admin.set_peer_tableCFs(@peer_id, table_cfs)
      assert_tablecfs_equal(table_cfs, replication_admin.get_peer_config(@peer_id).getTableCFsMap())

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "append_peer_tableCFs: works with table-cfs map" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      args = { CLUSTER_KEY => cluster_key}
      replication_admin.add_peer(@peer_id, args)

      replication_admin.set_peer_replicate_all(@peer_id, false)

      assert_equal(1, replication_admin.list_peers().length)
      assert_equal(cluster_key, replication_admin.list_peers().fetch(@peer_id).getClusterKey)

      table_cfs = { "table1" => [], "ns2:table2" => ["cf1"] }
      replication_admin.append_peer_tableCFs(@peer_id, table_cfs)
      assert_tablecfs_equal(table_cfs, replication_admin.get_peer_config(@peer_id).getTableCFsMap())

      table_cfs = { "table1" => [], "ns2:table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
      replication_admin.append_peer_tableCFs(@peer_id, { "ns3:table3" => ["cf1", "cf2"] })
      assert_tablecfs_equal(table_cfs, replication_admin.get_peer_config(@peer_id).getTableCFsMap())

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "remove_peer_tableCFs: works with table-cfs map" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      table_cfs = { "table1" => [], "ns2:table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
      args = { CLUSTER_KEY => cluster_key, TABLE_CFS => table_cfs }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      assert_equal(cluster_key, replication_admin.list_peers().fetch(@peer_id).getClusterKey)

      table_cfs = { "table1" => [], "ns2:table2" => ["cf1"] }
      replication_admin.remove_peer_tableCFs(@peer_id, { "ns3:table3" => ["cf1", "cf2"] })
      assert_tablecfs_equal(table_cfs, replication_admin.get_peer_config(@peer_id).getTableCFsMap())

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "set_peer_exclude_tableCFs: works with table-cfs map" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      args = { CLUSTER_KEY => cluster_key}
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      assert_equal(cluster_key, replication_admin.list_peers().fetch(@peer_id).getClusterKey)

      table_cfs = { "table1" => [], "table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
      replication_admin.set_peer_exclude_tableCFs(@peer_id, table_cfs)
      peer_config = replication_admin.get_peer_config(@peer_id)
      assert_equal(true, peer_config.replicateAllUserTables)
      assert_tablecfs_equal(table_cfs, peer_config.getExcludeTableCFsMap())

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "set_peer_namespaces: works with namespaces array" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      namespaces_str = "ns1;ns2"

      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)

      replication_admin.set_peer_replicate_all(@peer_id, false)

      replication_admin.set_peer_namespaces(@peer_id, namespaces)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "append_peer_namespaces: works with namespaces array" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      namespaces_str = "ns1;ns2"

      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)

      replication_admin.set_peer_replicate_all(@peer_id, false)

      replication_admin.append_peer_namespaces(@peer_id, namespaces)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      namespaces = ["ns3"]
      namespaces_str = "ns1;ns2;ns3"
      replication_admin.append_peer_namespaces(@peer_id, namespaces)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      # append a namespace which is already in the peer config
      replication_admin.append_peer_namespaces(@peer_id, namespaces)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "remove_peer_namespaces: works with namespaces array" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2", "ns3"]

      args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces }
      replication_admin.add_peer(@peer_id, args)

      replication_admin.set_peer_replicate_all(@peer_id, false)

      namespaces = ["ns1", "ns2"]
      namespaces_str = "ns3"
      replication_admin.remove_peer_namespaces(@peer_id, namespaces)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      namespaces = ["ns3"]
      namespaces_str = nil
      replication_admin.remove_peer_namespaces(@peer_id, namespaces)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      # remove a namespace which is not in peer config
      replication_admin.remove_peer_namespaces(@peer_id, namespaces)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "set_peer_exclude_namespaces: works with namespaces array" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      namespaces_str = "!ns1;ns2"

      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)

      replication_admin.set_peer_exclude_namespaces(@peer_id, namespaces)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(true, peer_config.replicateAllUserTables)
      assert_equal(namespaces_str,
        replication_admin.show_peer_exclude_namespaces(peer_config))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "set_peer_replicate_all" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"

      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers().length)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(true, peer_config.replicateAllUserTables)

      replication_admin.set_peer_replicate_all(@peer_id, false)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(false, peer_config.replicateAllUserTables)

      replication_admin.set_peer_replicate_all(@peer_id, true)
      peer_config = replication_admin.list_peers().fetch(@peer_id)
      assert_equal(true, peer_config.replicateAllUserTables)

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "set_peer_bandwidth: works with peer bandwidth upper limit" do
      cluster_key = "localhost:2181:/hbase-test"
      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)

      peer_config = replication_admin.get_peer_config(@peer_id)
      assert_equal(0, peer_config.get_bandwidth)
      replication_admin.set_peer_bandwidth(@peer_id, 2097152)
      peer_config = replication_admin.get_peer_config(@peer_id)
      assert_equal(2097152, peer_config.get_bandwidth)

      #cleanup
      replication_admin.remove_peer(@peer_id)
    end

    define_test "get_peer_config: works with simple clusterKey peer" do
      cluster_key = "localhost:2181:/hbase-test"
      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)
      peer_config = replication_admin.get_peer_config(@peer_id)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      #cleanup
      replication_admin.remove_peer(@peer_id)
    end

    define_test "get_peer_config: works with replicationendpointimpl peer and config params" do
      repl_impl = 'org.apache.hadoop.hbase.replication.ReplicationEndpointForTest'
      config_params = { "config1" => "value1", "config2" => "value2" }
      args = { ENDPOINT_CLASSNAME => repl_impl,
               CONFIG => config_params }
      replication_admin.add_peer(@peer_id, args)
      peer_config = replication_admin.get_peer_config(@peer_id)
      assert_equal(repl_impl, peer_config.get_replication_endpoint_impl)
      assert_equal(2, peer_config.get_configuration.size)
      assert_equal("value1", peer_config.get_configuration.get("config1"))
      #cleanup
      replication_admin.remove_peer(@peer_id)
    end

    define_test "list_peers: returns all peers Map<id, ReplicationPeerConfig>" do
      cluster_key = "localhost:2181:/hbase-test"
      args = { CLUSTER_KEY => cluster_key }
      peer_id_second = '2'
      replication_admin.add_peer(@peer_id, args)

      repl_impl = "org.apache.hadoop.hbase.replication.ReplicationEndpointForTest"
      config_params = { "config1" => "value1", "config2" => "value2" }
      args2 = { ENDPOINT_CLASSNAME => repl_impl, CONFIG => config_params}
      replication_admin.add_peer(peer_id_second, args2)

      peers = replication_admin.list_peers()
      assert_equal(2, peers.size)
      assert_equal(cluster_key, peers.fetch(@peer_id).get_cluster_key)
      assert_equal(repl_impl, peers.fetch(peer_id_second).get_replication_endpoint_impl)
      #cleanup
      replication_admin.remove_peer(@peer_id)
      replication_admin.remove_peer(peer_id_second)
    end

    define_test "update_peer_config: can update peer config and data" do
      repl_impl = "org.apache.hadoop.hbase.replication.ReplicationEndpointForTest"
      config_params = { "config1" => "value1", "config2" => "value2" }
      data_params = {"data1" => "value1", "data2" => "value2"}
      args = { ENDPOINT_CLASSNAME => repl_impl, CONFIG => config_params, DATA => data_params}
      replication_admin.add_peer(@peer_id, args)

      new_config_params = { "config1" => "new_value1" }
      new_data_params = {"data1" => "new_value1"}
      new_args = {CONFIG => new_config_params, DATA => new_data_params}
      replication_admin.update_peer_config(@peer_id, new_args)

      #Make sure the updated key/value pairs in config and data were successfully updated, and that those we didn't
      #update are still there and unchanged
      peer_config = replication_admin.get_peer_config(@peer_id)
      replication_admin.remove_peer(@peer_id)
      assert_equal("new_value1", peer_config.get_configuration.get("config1"))
      assert_equal("value2", peer_config.get_configuration.get("config2"))
      assert_equal("new_value1", Bytes.to_string(peer_config.get_peer_data.get(Bytes.toBytes("data1"))))
      assert_equal("value2", Bytes.to_string(peer_config.get_peer_data.get(Bytes.toBytes("data2"))))
    end
  end
end

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
require 'shell/formatter'
require 'hbase/hbase'
require 'hbase/table'

include HBaseConstants

module Hbase
  class GalaxyHelpersTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      @tmp_galaxy_str = '{"name": "test_galaxy_table", "atomicBatchSupport": false, '\
        '"primaryIndex": {"keyPartSpecs": [{"attribute": "~", "sortOrder": "ASC"}], '\
        '"columnFamily": "a"}, "columnFamilies": {"a": {"bloomFilterType": "ROW", '\
        '"blockSize": 65536, "maxVersions": 1, "keepDeletedCells": false, "replicationScope": 1, '\
        '"inMemory": false, "timeToLiveSec": 2147483647}}, "version": 1, '\
        '"attributes": {"C": {"dataType": "INT64", "repeated": false}, '\
        '"~": {"dataType": "INT64", "repeated": false}}, "validationMode": [], "entityGroup": {"enableHash": false}, '\
        '"columns": {"C": ["a"]}, "preSplits": 1}'
      @tmp_galaxy_path = '/tmp/test_galaxy_table.json'
      @test_galaxy_table = 'test_galaxy_table'
      File.open(@tmp_galaxy_path, 'w') do |f|
        f.write(@tmp_galaxy_str)
      end
      admin.galaxy_create(@tmp_galaxy_path) unless admin.exists?(@test_galaxy_table)
    end

    define_test "galaxy_describe" do
      admin.galaxy_describe(@test_galaxy_table, nil)
    end

    define_test "galaxy_describe should raise ArgumentError exceptions" do
      assert_raise(ArgumentError) do
        admin.galaxy_describe("not_exist_table_name", nil)
      end
    end

    define_test "galaxy_alter" do
      tmp_galaxy_str2 = @tmp_galaxy_str.sub('"version": 1', '"version": 2')
      File.open('/tmp/test_galaxy_table2.json', 'w') do |f|
        f.write(tmp_galaxy_str2)
      end
      admin.galaxy_alter('test_galaxy_table', '/tmp/test_galaxy_table2.json', true)
    end
  end
end

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
require 'hbase'
require 'hbase/hbase'
require 'hbase/table'

include HBaseConstants

module Hbase
  # Simple secure administration methods tests
  class QuotaAdminMethodsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)

      # Create table test table name
      @create_test_name = 'hbase_create_table_test_table'
    end

    define_test "Set region hard limit" do
      quota_admin.set_region_quota(REGION => 'abc', TYPE => READ, LIMIT => 100)
      quota_admin.list_region_quotas()
    end

    define_test "Remove region hard limit" do
      quota_admin.set_region_quota(REGION => 'abc', TYPE => READ, LIMIT => NONE)
    end

    define_test "Remove region hard limit without type" do
      quota_admin.set_region_quota(REGION => 'abc', LIMIT => NONE)
    end
  end
end

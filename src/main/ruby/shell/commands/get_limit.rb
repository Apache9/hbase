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
    class GetLimit < Command
      def help
        return <<-EOF
Get user throughput quota limits.
Syntax : get_limit <table>, <user>

When <table> is specified and not nil, the quota limits accounts for requests
made by one user to this table, otherwise, the quota limits accounts for all
requests made by one user.
If <user> is not specified, limits is returned for all users.

For example:

    hbase> get_limit 't1', 'bobsmith'
    hbase> get_limit
    hbase> get_limit nil, 'bobsmith'
EOF
      end

      def command(table_name=nil, user_name=nil)
        now = Time.now
        formatter.header(["User", "Quota"])
        count = quota_admin.get_limit(table_name, user_name) do |user_name, quota|
          formatter.row([user_name, quota])
        end
        formatter.footer(now, count)
      end
    end
  end
end

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
    class SetLimit < Command
      def help
        return <<-EOF
Set user throughput quota limits.
Syntax : set_limit <user> <limits> <table>

<limits> is a map of request types to quotas. Requests type includes Read('R')
and Write('W'). Quota is a numeric number which limits the request allowed
per second. Specially, a negative limit means there is no limit and can be
used to clear the previous limit.
When <table> is provided, the quota limits accounts for requests made by this
user to this specific table, otherwise, the quota limits accounts for all
requests made by this user.

For example:

    hbase> set_limit 'bobsmith', {'R' => 100, 'W' => 200}, 't1'
    hbase> set_limit 'bobsmith', 'R:20.0,W:40.0', 't2'
    hbase> set_limit 'bobsmith', {'R' => -1}
EOF
      end

      def command(user, limits, table_name=nil)
        format_simple_command do
          quota_admin.set_limit(user, limits, table_name)
        end
      end
    end
  end
end

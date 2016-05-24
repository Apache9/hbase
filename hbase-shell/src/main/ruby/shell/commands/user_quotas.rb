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
    class UserQuotas < Command
      def help
        return <<-EOF
List all user quota settings for table or namespace.
Syntax : user_quotas <table>

For example:

    hbase> user_quotas 'table1'
    hbase> user_quotas 'namespace1:table1'
    hbase> user_quotas '@ns1'
EOF
      end

      def command(table_regex)
        now = Time.now
        formatter.header(["OWNER", "QUOTAS"])

        count = 0
        if table_regex.start_with?('@')
          count += quotas_admin.list_quotas({"NAMESPACE" => table_regex[1..-1]}) do |row, cells|
            formatter.row([ row, cells ])
          end
          count = quotas_admin.list_quotas({"USER" => '.*', "TABLE" => table_regex[1..-1] + ":.*"}) do |row, cells|
            formatter.row([ row, cells ])
          end
        else  
          count += quotas_admin.list_quotas({"USER" => '.*', "TABLE" => table_regex}) do |row, cells|
            formatter.row([ row, cells ])
          end
        end

        formatter.footer(now, count)
      end
    end
  end
end

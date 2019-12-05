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
    class SetRegionQuota < Command
      def help
        return <<-EOF
Set or remove region quota which is a hard limit.
NOTE: This will work whether quota is enabled or disabled.

Syntax : set_region_quota REGION => 'ENCODED_REGIONNAME', TYPE => <type>, LIMIT => <value> 
For example:
    # set region read limit to 1000
    hbase> set_region_quota REGION => 'ENCODED_REGIONNAME', TYPE => READ, LIMIT => 1000

    # remove region write limit
    hbase> set_region_quota REGION => 'ENCODED_REGIONNAME', TYPE => WRITE, LIMIT => NONE

    # remove region read and write limit if type is not specified
    hbase> set_region_quota REGION => 'ENCODED_REGIONNAME', LIMIT => NONE
EOF
      end

      def command(args = {})
        format_simple_command do
          quotas_admin.set_region_quota(args)
        end
      end
    end
  end
end

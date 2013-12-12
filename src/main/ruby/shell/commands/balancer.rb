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

module Shell
  module Commands
    class Balancer < Command
      def help
        return <<-EOF
Trigger the cluster balancer or the table balancer if table name is sepecified. Returns true if
balancer ran and was able to tell the region servers to unassign all the regions to balance 
(the re-assignment itself is async). Otherwise false (Will not run if regions in transition).
Examples:

  hbase> balancer             # trigger cluster balancer 
  hbase> balancer 'TABLENAME' # trigger table balancer
EOF
      end

      def command(table_name = nil)
        format_simple_command do
          formatter.row([
            admin.balancer(table_name)? "true": "false"
          ])
        end
      end
    end
  end
end

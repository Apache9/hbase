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
    class SetPeerExcludedTableCFs< Command
      def help
        return <<-EOF
Set the excluded table-cfs config for the specified peer
Examples:

  # set table / table-cf to be excluded for a peer
  # table1 and table2:cf1,cf2 will not be replicated to slave cluster
  hbase> set_peer_tableCFs '2', "table1;table2:cf1,cf2"

EOF
      end

      def command(id, table_cfs)
        format_simple_command do
          replication_admin.set_peer_excluded_tableCFs(id, table_cfs)
        end
      end
    end
  end
end

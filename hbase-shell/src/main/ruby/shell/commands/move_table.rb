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
    class MoveTable < Command
      def help
        return <<-EOF
Move all regions of the table to a regionserver. All regions will be moved to the
given regionserver if the 'SERVER_NAME' is specified, or to a random regionserver if
'SERVER_NAME' is absent

NOTE:
A server name is its host, port plus startcode. For example:
host187.example.com,60020,1289493121758
Examples:

  hbase> move_table 'TABLENAME', 'SERVER_NAME'
  hbase> move_table 'TABLENAME'
EOF
      end

      def command(table_name, server_name = nil)
        format_simple_command do
          admin.move_table(table_name, server_name)
        end
      end
    end
  end
end

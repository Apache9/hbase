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
    class MoveServer < Command
      def help
        return <<-EOF
Move all regions of the source server to a target servers list, or randomly to
other servers if no target server specified.
The last parameter is time interval between moving regions. When moved one region,
first sleep time interval seconds, then move another region. The default time interval is 1s.

NOTE:
A server name is its host, port plus startcode. For example:
host187.example.com,60020,1289493121758
Examples:

  # move all regions of SOURCE_SEREVR to TARGET_SERVER1 and TARGET_SERVER2, time interval is 10s
  hbase> move_server 'SOURCE_SEREVR', ['TARGET_SERVER1', 'TARGET_SERVER2'], 10

  hbase> move_server 'SOURCE_SEREVR', ['TARGET_SERVER1', 'TARGET_SERVER2', 'TARGET_SERVER3']
  hbase> move_server 'SOURCE_SERVER'
EOF
      end

      def command(source_server, target_servers = [], time_interval = 0)
        format_simple_command do
          admin.move_server(source_server, target_servers, time_interval)
        end
      end
    end
  end
end

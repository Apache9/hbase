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
    class StopThrottle < Command
      def help
        return <<-EOF
Stop throttle by quota.
NOTE: if quota is not enabled, this will not work.
  
Syntax : stop_throttle
For example:

    hbase> stop_throttle
EOF
      end

      def command()
        format_simple_command do
          state = admin.stop_throttle()
          formatter.row([
            state == nil ? "Quota is not enabled" : state.name + " ==> OFF"
          ])
        end
      end
    end
  end
end

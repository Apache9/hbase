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
    class ShowPeerConfiguration< Command
      def help
        return <<-EOF
Show the configuration of a special peer.

  hbase> show_peer_configuration '1'

        EOF
      end

      def command(id)
        now = Time.now
        formatter.header(["KEY", "VALUE"])
        peerConfig = replication_admin.get_peer_config(id)
        peerConfig.getConfiguration().each{|key, val|
          formatter.row([key, val])
        }
        formatter.footer(now)
      end
    end
  end
end

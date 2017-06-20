#
# Copyright The Apache Software Foundation
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
    class SetPeerReplicateAll< Command
      def help
        return <<-EOF
  Set the replicate_all flag to true or false for the specified peer.
  
  If replicate_all flag is true, then all user tables (REPLICATION_SCOPE != 0)
  will be replicate to peer cluster. But you can set EXCLUDE_NAMESPACES or
  EXCLUDE_TABLECFS to exclude some tables which not replicated.

  If replicate_all flag is false, then all user tables cannot be replicate to
  peer cluster. But you can set NAMESPACES or TABLECFS to include some tables
  which will be replicated.

  Notice:
  1. When you want to change a peer's replicate_all flag from true to false, you need clean
     the peer's EXCLUDE_NAMESPACES and EXCLUDE_TABLECFS config firstly.
  2. When you want to change a peer's replicate_all flag from false to true, you need clean
     the peer's NAMESPACES and TABLECFS config firstly.

  Examples:

    # set replicate_all flag to true
    hbase> set_peer_replicate_all '1', true
    # set replicate_all flag to false
    hbase> set_peer_replicate_all '1', false

  EOF
      end

      def command(id, replicate_all)
        format_simple_command do
          replication_admin.set_peer_replicate_all(id, replicate_all)
        end
      end
    end
  end
end

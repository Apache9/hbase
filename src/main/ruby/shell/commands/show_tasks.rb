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
    class ShowTasks < Command
      def help
        return <<-EOF
Show task status
Examples:
  hbase> show_tasks
  hbase> show_tasks 'CONDITIONS' 

EOF
      end

      def command(conditions = nil)
        tasks = admin.show_tasks(conditions);
        java.lang.System.out.println("SERVER\tSTART_TIME\tDESCRIPTION\tSTATE\tSTATUS");

        tasks.entrySet().each do |e|
          server = e.key
          rsTasks = e.value
            rsTasks.each do |v|

              now = Time.now.to_i
              sinceState = "" + (now - v.stateTime/1000).to_s
              sinceStatus = "" + (now - v.statusTime/1000).to_s

              java.lang.System.out.println(server.to_s << "\t" << Time.at(v.startTime).to_s << "\t" << v.description << "\t" <<  v.state.name << "(since " << sinceState <<"sec ago)\t" << v.status << "(since " << sinceStatus  << "sec ago)" )
            end
        end
      end
    end
  end
end

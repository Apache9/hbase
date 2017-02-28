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
    class GalaxyAlterSlave < Command
      def help
        return <<-EOF
Alter Galaxy SDS table schema and set slave flag; Pass table name and a new
schema file path with json format.

Example:
  hbase> galaxy_alter_slave 't1', 't1.json'
EOF
      end

      def command(table, schema_file)
        format_simple_command do
          admin.galaxy_alter(table, schema_file, true, true)
        end
      end
    end
  end
end

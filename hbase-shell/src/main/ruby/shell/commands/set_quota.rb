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
    class SetQuota < Command
      def help
        return <<-EOF
Set a quota for a (user, table), or namespace.
Syntax : set_quota TYPE => <type>, <args>

User can either set quota on read, write(TYPE => READ or TYPE => WRIET). The quota is
expressed using limit, such as LIMIT => 1000 means up to 1000 times read or write request
is allowed.

Usually, global admins need to set quota for namespace, such as:
    # set quota for namespace, all the users can issue at most 100 read requests and
    # 1000 write requests to tables under ns1
    hbase> set_quota TYPE => READ, NAMESPACE => 'ns1', LIMIT => 100 
    hbase> set_quota TYPE => WRITE, NAMESPACE => 'ns1', LIMIT => 1000

global admins can also cancel quota setting for namespace:
    # cancel read quota setting for ns1
    hbase> set_quota TYPE => READ, NAMESPACE => 'ns1', LIMIT => NONE
    # cancel write quota setting for ns1
    hbase> set_quota TYPE => WRITE, NAMESPACE => 'ns1', LIMIT => NONE
    # cancle all quota settings for ns1
    hbase> set_quota NAMESPACE => 'ns1', LIMIT => NONE

namespace admins need to set quota for the user and table(under the namespace):
    # set quota for (user, table): u1 can issue at most 10 read requests to ns1:t1 per second
    hbase> set_quota TYPE => READ, USER => 'u1', TABLE => 'ns1:t1', LIMIT => 10
    # set quota for (user, table): u2 can issue at most 100 write requests to ns1:t1 for second
    hbase> set_quota TYPE => WRITE, USER => 'u2', TABLE => 'ns1:t1', LIMIT => 100 

namespace admins can cancel quota setting for user and table(under the namespace):
    # cancel read quota setting for (u1, ns1:t1):
    hbase> set_quota TYPE => READ, USER => 'u1', TABLE => 'ns1:t1', LIMIT => NONE 
    # cancel write quota setting for (u2, ns1:t1):
    hbase> set_quota TYPE => WRITE, USER => 'u2', TABLE => 'ns1:t1', LIMIT => NONE
    # cancel all quota settings for (u1, ns1:t1)
    hbase> set_quota USER => 'u1', TABLE => 'ns1:t1', LIMIT => NONE
    # cancel all related quota settings about ns1:t1
    hbase> set_quota TABLE => 'ns1:t1', LIMIT => NONE

Meanwhile, gobal admins can bypass quota check for user and table:
    # bypass quota check when u1 access ns1:t1
    hbase> set_quota GLOBAL_BYPASS => true, USER => 'u1', TABLE => 'ns1:t1'
    # recovery quota check when u1 access ns1:t1
    hbase> set_quota GLOBAL_BYPASS => false, USER => 'u1', TABLE => 'ns1:t1'
    # bypass quota check for user u1
    hbase> set_quota GLOBAL_BYPASS => true, USER => 'u1'
    # recovery quota check for user u1
    hbase> set_quota GLOBAL_BYPASS => false, USER => 'u1'

EOF
      end

      def command(args = {})
        if args.has_key?(GLOBAL_BYPASS)
          quotas_admin.set_global_bypass(args.delete(GLOBAL_BYPASS), args)
        else
          if args[LIMIT].eql? NONE
            args.delete(LIMIT)
            quotas_admin.unthrottle(args)
          else
            quotas_admin.throttle(args)
          end
        end
      end
    end
  end
end

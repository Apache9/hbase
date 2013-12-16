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

include Java

# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class QuotaAdmin
    include HBaseConstants

    def initialize(configuration, formatter)
      @config = configuration
      @admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(configuration)
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    def parse_limits(limits)
      if limits.is_a?(String)
        h = {}
        limits.split(',').each do |item|
          parts = item.split ':'
          if parts.length != 2
            raise(ArgumentError, "Invalid quota format: #{limits}")
          end
          h[parts[0]] = parts[1].to_f
        end
        limits = h
      end

      if ! limits.is_a? Hash
        raise(ArgumentError, "Invalid quota format: #{limits}")
      end

      limits_by_type = {}
      limits.each do |code, limit|
        if code.length != 1
          raise(ArgumentError, "Invalid request type: #{code}")
        end
        reqest_type = org.apache.hadoop.hbase.throughput.RequestType.fromCode(code.upcase.to_java_bytes[0])
        if reqest_type.nil?
          raise(ArgumentError, "Invalid request type: #{code}")
        end
        if limit > java.lang.Double::MAX_VALUE
          raise(ArgumentError, "Limit too large: #{limit}")
        end
        limit = -1 if limit < 0
        limits_by_type[reqest_type] = limit.to_f
      end
      if !limits_by_type.empty?
        java.util.EnumMap.new(limits_by_type)
      else
        java.util.EnumMap.new(org.apache.hadoop.hbase.throughput.RequestType.java_class)
      end
    end

    #----------------------------------------------------------------------------------------------
    def set_limit(user_name, limits, table_name=nil)
      throughput_quota_available?

      if (user_name.nil? || user_name.empty?)
        raise(ArgumentError, "Empty user")
      end

      if (!table_name.nil?)
        # Set quota limits for requests to this table. Table should exist
        raise(ArgumentError, "Can't find table: #{table_name}") unless exists?(table_name)
      else
        # Set quota limits for all requests
        table_name = org.apache.hadoop.hbase.throughput.ThroughputQuotaTable::WILDCARD_TABLE_NAME_STR
      end

      meta_table = org.apache.hadoop.hbase.client.HTable.new(@config,
                      org.apache.hadoop.hbase.throughput.ThroughputQuotaTable::THROUGHPUT_QUOTA_TABLE_NAME)
      protocol = meta_table.coprocessorProxy(
                      org.apache.hadoop.hbase.throughput.ThroughputControllerProtocol.java_class,
                                             org.apache.hadoop.hbase.HConstants::EMPTY_START_ROW)
      quota = org.apache.hadoop.hbase.throughput.ThroughputQuota.new(table_name, {user_name => parse_limits(limits)})
      protocol.setThroughputLimit(quota)
    end

    #----------------------------------------------------------------------------------------------
    def get_limit(table_name=nil, user_name=nil)
      throughput_quota_available?

      if (!table_name.nil?)
        # Set quota limits for requests to this table. Table should exist
        raise(ArgumentError, "Can't find table: #{table_name}") unless exists?(table_name)
      else
        # Set quota limits for all requests
        table_name = org.apache.hadoop.hbase.throughput.ThroughputQuotaTable::WILDCARD_TABLE_NAME_STR
      end

      meta_table = org.apache.hadoop.hbase.client.HTable.new(@config,
                      org.apache.hadoop.hbase.throughput.ThroughputQuotaTable::THROUGHPUT_QUOTA_TABLE_NAME)
      protocol = meta_table.coprocessorProxy(
                      org.apache.hadoop.hbase.throughput.ThroughputControllerProtocol.java_class,
                                             org.apache.hadoop.hbase.HConstants::EMPTY_START_ROW)
      quota = protocol.getThroughputLimit(table_name.to_java_bytes)

      if quota.nil?
        return (block_given?) ? 0 : []
      end

      all_limits = quota.get_limits
      if (user_name.nil? || user_name.empty?)
        users = all_limits.keys
      else
        # If user_name is specified, just discards all others
        users = [user_name]
      end

      count = 0
      result = []
      users.each do |user|
        limits = all_limits.get(user)
        if !limits.nil? && !limits.empty?
          if block_given?
            count += 1
            yield(user, "#{limits.to_string}")
          else
            result << [user, "#{limits.to_string}"]
          end
        end
      end
      (block_given?) ? count : result
    end

    # Does table exist?
    def exists?(table_name)
      @admin.tableExists(table_name)
    end

    # Make sure that throughput quota classes are available
    def throughput_quota_available?()
      begin
        org.apache.hadoop.hbase.throughput.ThroughputControllerProtocol
      rescue NameError
        raise(ArgumentError, "DISABLED: Throughput quota features are not available in this build of HBase")
      end
    end

  end
end

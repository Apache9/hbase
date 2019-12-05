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

include Java
java_import java.util.concurrent.TimeUnit
java_import org.apache.hadoop.hbase.TableName
java_import org.apache.hadoop.hbase.quotas.ThrottleType
java_import org.apache.hadoop.hbase.quotas.QuotaFilter
java_import org.apache.hadoop.hbase.quotas.QuotaRetriever
java_import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory

module HBaseQuotasConstants
  GLOBAL_BYPASS = 'GLOBAL_BYPASS'
  TYPE = 'TYPE'
  THROTTLE = 'THROTTLE'
  REQUEST = 'REQUEST'
  WRITE = 'WRITE'
  READ = 'READ'
  REGION = 'REGION'
end

module Hbase
  class QuotasAdmin
    def initialize(configuration, formatter)
      @config = configuration
      @admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(configuration)
      @formatter = formatter
    end

    def throttle(args)
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)
      type = ThrottleType.valueOf(args.delete(TYPE) + "_NUMBER")
      limit = args.delete(LIMIT)
      time_unit = TimeUnit::SECONDS

      if args.has_key?(USER)
        user = args.delete(USER)
        if args.has_key?(TABLE)
          table = TableName.valueOf(args.delete(TABLE))
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.throttleUser(user, table, type, limit, time_unit)
        else
          raise(ArgumentError, "Must specify table") 
        end
      elsif args.has_key?(NAMESPACE)
        namespace = args.delete(NAMESPACE)
        raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.throttleNamespace(namespace, type, limit, time_unit)
      else
        raise "One of USER=>TABLE or NAMESPACE must be specified"
      end
      @admin.setQuota(settings)
    end

    def unthrottle(args)
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)
      if args.has_key?(TYPE)
        type = ThrottleType.valueOf(args.delete(TYPE) + "_NUMBER")
      end
      if args.has_key?(USER)
        user = args.delete(USER)
        if args.has_key?(TABLE)
          table = TableName.valueOf(args.delete(TABLE))
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.unthrottleUser(user, table, type)
        else
          raise(ArgumentError, "Must specify table") 
        end
      elsif args.has_key?(TABLE)
        table = args.delete(TABLE)
        raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
        unthrottle_table(table)
        return
      elsif args.has_key?(NAMESPACE)
        namespace = args.delete(NAMESPACE)
        raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.unthrottleNamespace(namespace, type)
      else
        raise "One of USER=>TABLE , TABLE or NAMESPACE must be specified"
      end
      @admin.setQuota(settings)
    end

    def unthrottle_table(table)
      filter = QuotaFilter.new()
      filter.setUserFilter("(.+)").setTableFilter(table)
      $stdout.puts "start cancel all related quota settings about table #{table}..."

      scanner = @admin.getQuotaRetriever(filter)
      begin
        iter = scanner.iterator
        # Iterate results
        while iter.hasNext
          settings = iter.next
          user = settings.getUserName()
          table = settings.getTableName()
          @admin.setQuota(QuotaSettingsFactory.unthrottleUser(user, table))
          $stdout.puts "finish unthrottling for (USER => #{user}, TABLE => #{table})."
        end
      ensure
        scanner.close()
      end
      $stdout.puts "finish cancel all related quota settings about table #{table}."
    end

    def set_global_bypass(bypass, args)
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)

      if args.has_key?(USER)
        user = args.delete(USER)
        if args.has_key?(TABLE)
          table = TableName.valueOf(args.delete(TABLE))
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.bypassGlobals(user, table, bypass)
        else
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.bypassGlobals(user, bypass)
        end
      else
        raise "Expected USER"
      end
      @admin.setQuota(settings)
    end

    def list_quotas(args = {})
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)

      limit = args.delete("LIMIT") || -1
      count = 0

      filter = QuotaFilter.new()
      filter.setUserFilter(args.delete(USER)) if args.has_key?(USER)
      filter.setTableFilter(args.delete(TABLE)) if args.has_key?(TABLE)
      filter.setNamespaceFilter(args.delete(NAMESPACE)) if args.has_key?(NAMESPACE)
      raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?

      # Start the scanner
      scanner = @admin.getQuotaRetriever(filter)
      begin
        iter = scanner.iterator

        # Iterate results
        while iter.hasNext
          if limit > 0 && count >= limit
            break
          end

          settings = iter.next
          owner = {
            USER => settings.getUserName(),
            TABLE => settings.getTableName(),
            NAMESPACE => settings.getNamespace(),
          }.delete_if { |k, v| v.nil? }.map {|k, v| k.to_s + " => " + v.to_s} * ', '

          yield owner, settings.to_s

          count += 1
        end
      ensure
        scanner.close()
      end

      return count
    end

    def _parse_size(str_limit)
      str_limit = str_limit.downcase
      match = /(\d+)([bkmgtp%]*)/.match(str_limit)
      if match
        if match[2] == '%'
          return match[1].to_i
        else
          return _size_from_str(match[1].to_i, match[2])
        end
      else
        raise "Invalid size limit syntax"
      end
    end

    #def _parse_limit(str_limit, type_cls, type)
    #  str_limit = str_limit.downcase
    #  match = /(\d+)(req|[bkmgtp])\/(sec|min|hour|day)/.match(str_limit)
    #  if match
    #    if match[2] == 'req'
    #      limit = match[1].to_i
    #      type = type_cls.valueOf(type + "_NUMBER")
    #    else
    #      limit = _size_from_str(match[1].to_i, match[2])
    #      type = type_cls.valueOf(type + "_SIZE")
    #    end

    #    if limit <= 0
    #      raise "Invalid throttle limit, must be greater then 0"
    #    end

    #    case match[3]
    #      when 'sec'  then time_unit = TimeUnit::SECONDS
    #      when 'min'  then time_unit = TimeUnit::MINUTES
    #      when 'hour' then time_unit = TimeUnit::HOURS
    #      when 'day'  then time_unit = TimeUnit::DAYS
    #    end

    #    return type, limit, time_unit
    #  else
    #    raise "Invalid throttle limit syntax"
    #  end
    #end

    def _size_from_str(value, suffix)
      case suffix
        when 'k' then value <<= 10
        when 'm' then value <<= 20
        when 'g' then value <<= 30
        when 't' then value <<= 40
        when 'p' then value <<= 50
      end
      return value
    end

    def set_region_quota(args)
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)
      region = args.delete(REGION)
      if args.has_key?(TYPE)
        type = ThrottleType.valueOf(args.delete(TYPE) + "_NUMBER")
        if args[LIMIT].eql? NONE
          @admin.removeRegionQuota(region.to_java_bytes, type)
        elsif
          limit = args.delete(LIMIT)
          @admin.setRegionQuota(region.to_java_bytes, type, limit, TimeUnit::SECONDS)
        end
      elsif
        @admin.removeRegionQuota(region.to_java_bytes)
      end
    end

    def list_region_quotas
      count  = 0
      list = @admin.listRegionQuota
      list.each do |region_quota|
        yield(region_quota.getRegionName, region_quota.getThrottleType, region_quota.getLimit) if block_given?
      count += 1
      end
      return count
    end

  end
end

require 'optparse'
require 'java'

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.security.access.AccessControlClient
import org.apache.hadoop.hbase.security.access.AccessControlLists
import org.apache.hadoop.hbase.HColumnDescriptor

log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

$config = nil # HBaseConfiguration instance
$hbase_admin = nil # HBaseAdmin instance
$output_file_name = nil # output file where grants dump to

def security_available?()
  if !table_exists(AccessControlLists::ACL_TABLE_NAME)
    raise(ArgumentError, "DISABLED: Security features are not available")
  end
end

def init_hbase_admin()
  $config = HBaseConfiguration.create
  $hbase_admin = HBaseAdmin.new($config)
end

def output(str)
  if $output_file_name == nil
    puts str
    puts
  elsif
    File.open($output_file_name, 'a') do |f|
      f.puts str
      f.puts
      f.close
    end
  end
end

def is_namespace(table_regex)
  return table_regex.start_with?('@')
end

def table_exists(table_name)
  return $hbase_admin.tableExists(table_name)
end

def namespace_exists(namespace_name)
  begin
    $hbase_admin.getNamespaceDescriptor(namespace_name)
    return true
  rescue
    return false
  end
end

def list_tables(namespace_regex=nil)
  table_names = []
  if namespace_regex == nil || namespace_regex.length == 0
    $hbase_admin.listTableNames.each do |table_name|
      table_names.push(table_name.to_s)
    end
  else
    $hbase_admin.listTableNamesByNamespace(namespace_regex).each do |table_name|
      table_names.push(table_name.to_s)
    end
  end
  return table_names
end

def list_namespace()
  namespaces = []
  $hbase_admin.listNamespaceDescriptors.each do |ns|
    namespaces.push(ns.getName)
  end
  return namespaces
end

def dump_table_privileges(table_regex)
  security_available?

  if table_regex != nil && is_namespace(table_regex)
    if !namespace_exists(table_regex[1...table_regex.length])
      raise(ArgumentError, "Namespace: #{table_regex} not exist")
    end
  elsif table_regex != nil && table_regex.length > 0
    if !table_exists(table_regex)
      raise(ArgumentError, "Table: #{table_regex} not exist")
    end
  end

  permList = AccessControlClient.getUserPermissions($config, table_regex)
  grants = []
  header = ''
  permList.each do |value|
    grant = []
    user_name = String.from_java_bytes(value.getUser)
    grant.push("'#{user_name}'")

    actions = ''
    value.getActions.each do |action|
      actions.concat(action.code())
    end

    grant.push("'#{actions}'")

    if (table_regex != nil && is_namespace(table_regex))
      header = "### dump namespace grants:  #{table_regex}"
      grant.push("'#{table_regex}'")
    elsif value.getTableName != nil

      header = "### dump table grants:  #{table_regex}"
      grant.push("'#{value.getTableName.getNameAsString()}'")

      if value.getFamily != nil
        grant.push("'#{Bytes::toStringBinary(value.getFamily)}'")
      end

      if value.getQualifier != nil
        grant.push("'#{Bytes::toStringBinary(value.getQualifier)}'")
      end

    end
    grants.push("grant #{grant.join(', ')}")
  end

  grants.insert(0, header)

  return grants
end

def dump_all()
  list_namespace.each do |ns|
    dump_namespace(ns)
  end
end

def dump_namespace(namespace)
  # dump namespace privilegs first
  output dump_table_privileges("@#{namespace}")

  # dump table privileges under namespace
  list_tables(namespace).each do |table|
    output dump_table_privileges(table)
  end
end

# Do command-line parsing
cmd_help = <<HERE
Dump HBase Table/Namespace grants

Example:
  bash hbase-jruby hbase_dump.rb -h | --help               # show usage
  bash hbase-jruby hbase_dump.rb t1                        # dump permission of table t1
  bash hbase-jruby hbase_dump.rb t1 t2                     # dump permission of table t1 & t2
  bash hbase-jruby hbase_dump.rb -w <file> t1 t2           # dump permission of table t1 & t2 to <file>
  bash hbase-jruby hbase_dump.rb t1 @ns1 t2                # dump permission of table t1 & t2, namespace ns1
  bash hbase-jruby hbase_dump.rb -w <file> t1 @ns1 t2      # dump permission of table t1 & t2, namespace ns1 to <file>
  bash hbase-jruby hbase_dump.rb --namespace ns1           # dump permission of all table under namespace
  bash hbase-jruby hbase_dump.rb --namespace ns1 -w <file> # dump permission of all table under namespace to <file>
  bash hbase-jruby hbase_dump.rb --all                     # dump permission of all table
  bash hbase-jruby hbase_dump.rb --all -w <file>           # dump permission of all table to <file>
HERE

begin

  if ARGV.length < 1
    puts cmd_help
    exit
  end

  is_dump_all = false
  dump_namespace = nil

  i = 0
  while i < ARGV.length
    if ARGV[i] == '--all' or ARGV[i] == '-a'
      is_dump_all = true
    elsif ARGV[i] == '--namespace' or ARGV[i] == '-n'
      i += 1
      if i >= ARGV.length
        puts "ERROR: set '--namespace' option, but no namespace found"
        puts cmd_help
        exit
      end
      dump_namespace = ARGV[i]
    elsif ARGV[i] == '-w'
      i += 1
      if i >= ARGV.length
        puts "ERROR: set '-w' option, but no output file found"
        puts cmd_help
        exit
      end
      $output_file_name = ARGV[i]
    else
      break
    end
    i += 1
  end

  if $output_file_name != nil && File.file?($output_file_name)
    File.delete($output_file_name)
  end

  # initailize hbase admin instance
  init_hbase_admin

  if is_dump_all
    dump_all
  elsif  dump_namespace
    dump_namespace(dump_namespace)
  elsif
    ARGV.slice(i, ARGV.length).each do |table_name|
      output dump_table_privileges(table_name)
    end
  end

rescue Exception=>e
  if "#{e}" != 'exit'
    puts "#{e}"
  end
end

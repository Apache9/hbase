include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.security.access.AccessControlLists

log_level = org.apache.log4j.Level::OFF
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase.security").setLevel(log_level)

$config = nil # HBaseConfiguration
$hbase_admin = nil # HBaseAdmin instance
$output_file_name = nil # output file where grants dump to.

def security_available?()
  if !table_exists(AccessControlLists::ACL_TABLE_NAME)
    raise(ArgumentError, "DISABLED: Security features are not available")
  end
end

def init_hbase_admin()
  $config = HBaseConfiguration.create
  $hbase_admin = HBaseAdmin.new($config)
end

def table_exists(table_name)
  return $hbase_admin.tableExists(table_name)
end

def list_tables()
  table_names = []
  $hbase_admin.listTables.each do |table_name|
    table_names.push(table_name.getNameAsString)
  end
  return table_names
end

def dump_table_privileges(table_regex)
  security_available?

  if table_regex != nil && table_regex.length > 0
    if !table_exists(table_regex)
      raise(ArgumentError, "Table: #{table_regex} not exist")
    end
  end

  meta_table = HTable.new($config, AccessControlLists::ACL_TABLE_NAME)
  protocol = meta_table.coprocessorProxy(
      org.apache.hadoop.hbase.security.access.AccessControllerProtocol.java_class,
      org.apache.hadoop.hbase.HConstants::EMPTY_START_ROW)
  permList = protocol.getUserPermissions(table_regex != nil ? table_regex.to_java_bytes : nil)

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

    if value.getTable != nil

      header = "### dump table grants:  #{table_regex}"
      grant.push("'#{Bytes::toStringBinary(value.getTable)}'")

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

def output_to_file(str)
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

def dump_all()
  list_tables.each do |table|
    output_to_file dump_table_privileges(table)
  end
end

# Do command-line parsing
cmd_help = <<HERE
Dump HBase Table grants

Example:
  bash ./bin/hbase-jruby hbase_dump.rb -h | --help      # show usage
  bash ./bin/hbase-jruby hbase_dump.rb t1               # dump permission of table t1
  bash ./bin/hbase-jruby hbase_dump.rb t1 t2            # dump permission of table t1 & t2
  bash ./bin/hbase-jruby hbase_dump.rb -w <file> t1 t2  # dump permission of table t1 & t2 to <file>
  bash ./bin/hbase-jruby hbase_dump.rb --all            # dump permission of all table
  bash ./bin/hbase-jruby hbase_dump.rb -w <file> --all  # dump permission of all table to <file>
HERE

begin

  if ARGV.length < 1
    puts cmd_help
    exit
  end

  is_dump_all = false

  i = 0
  while i < ARGV.length
    if ARGV[i] == '-w'
      i += 1
      if i >= ARGV.length
        puts "ERROR: set '-w' flag, but output file name not found."
      end
      $output_file_name = ARGV[i]
    elsif ARGV[i] == '--all'
      is_dump_all = true
    elsif ARGV[i] == '-h' || ARGV[i] == '--help'
      puts cmd_help
      exit
    else
      break
    end
    i += 1
  end

  if $output_file_name != nil && File.file?($output_file_name)
    File.delete($output_file_name)
  end

  if is_dump_all
    if i < ARGV.length
      puts "ERROR: Unexpected arguments, #{ARGV.slice(i, ARGV.length)}"
      puts cmd_help
      exit
    end
    init_hbase_admin
    dump_all()
  else
    init_hbase_admin
    ARGV.slice(i, ARGV.length).each do |table_name|
      output_to_file dump_table_privileges(table_name)
    end
  end

rescue Exception=>e
  if "#{e}" != 'exit'
    puts "#{e}"
  end
end

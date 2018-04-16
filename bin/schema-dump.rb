require 'java'

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin

log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

$config = nil # HBaseConfiguration instance
$hbase_admin = nil # HBaseAdmin instance/
$output_file_name = nil # output file where grants dump to

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

def list_tables(namespace_regex)
  table_names = []
  $hbase_admin.listTableNamesByNamespace(namespace_regex).each do |table_name|
    table_names.push(table_name.to_s)
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

def dump_table_schema(table_name)
  tableDesc = $hbase_admin.getTableDescriptor(table_name.to_java_bytes)
  table_conf = tableDesc.toStringTableAttributes
  column_families = tableDesc.getColumnFamilies
  buf = []
  column_families.each do |column_family|
    buf.push(column_family.to_s)
  end
  families = buf.join(", \\\n")
  return "create '#{table_name}', #{table_conf}, #{families}" \
             .gsub(/\{TABLE_ATTRIBUTES =>/, '') \
             .gsub(/TTL => \'FOREVER\',/, '') \
             .gsub(/TTL => \'FOREVER\'/, '')
end

def dump_all()
  list_namespace.each do |ns|
    dump_namespace(ns)
  end
end

def dump_namespace(namespace)
  # dump table privileges under namespace
  list_tables(namespace).each do |table|
    output dump_table_schema(table)
  end
end

# Do command-line parsing
cmd_help = <<HERE
Dump HBase Table/Namespace grants

Example:
  bash hbase-jruby schema-dump.rb -h | --help               # show usage
  bash hbase-jruby schema-dump.rb t1                        # dump schema of table t1
  bash hbase-jruby schema-dump.rb t1 t2                     # dump schema of table t1 & t2
  bash hbase-jruby schema-dump.rb -w <file> t1 t2           # dump schema of table t1 & t2 to <file>
  bash hbase-jruby schema-dump.rb t1 @ns1 t2                # dump schema of table t1 & t2, namespace ns1
  bash hbase-jruby schema-dump.rb -w <file> t1 @ns1 t2      # dump schema of table t1 & t2, namespace ns1 to <file>
  bash hbase-jruby schema-dump.rb --namespace ns1           # dump schema of all table under namespace
  bash hbase-jruby schema-dump.rb --namespace ns1 -w <file> # dump schema of all table under namespace to <file>
  bash hbase-jruby schema-dump.rb --all                     # dump schema of all table
  bash hbase-jruby schema-dump.rb --all -w <file>           # dump schema of all table to <file>
HERE

begin

  if ARGV.length < 1
    puts cmd_help
    exit
  end

  is_dump_all = false
  namespace_to_dump = nil

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
      namespace_to_dump = ARGV[i]
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
  elsif  namespace_to_dump
    dump_namespace(namespace_to_dump)
  elsif
    ARGV.slice(i, ARGV.length).each do |table_name|
      output dump_table_schema(table_name)
    end
  end

rescue Exception=>e
  if "#{e}" != 'exit'
    puts "#{e}"
  end
end

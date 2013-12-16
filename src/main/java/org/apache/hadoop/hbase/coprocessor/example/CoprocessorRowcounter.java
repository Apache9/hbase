/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A Tool to count the rows of table using coprocessor.
 * 
 * Set "coprocessor.rowcounter.sink" as the way you want to output result.
 * If choose HBaseSink, set "coprocessor.rowcounter.table" as the target table.
 */
public class CoprocessorRowcounter extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(CoprocessorRowcounter.class);

  private static Configuration conf;
  private Sink sink;
  
  private String tableName = null;
  private String startRow = null;
  private String stopRow = null;
  private ArrayList<String> columnFamilies = null;
  private int speed = -1;

  public CoprocessorRowcounter(){
    conf = HBaseConfiguration.create();
    String sinkClassName = conf.get("coprocessor.rowcounter.sink", "org.apache.hadoop.hbase.coprocessor.example.CoprocessorRowcounter$StdOutSink");
    try {
      sink = (Sink) Class.forName(sinkClassName).newInstance();
    } catch (Exception e) {
      LOG.error("error when reflect sink class", e);
    }
  }
  
  public interface Sink{
    public void publishResult(String tableName, String date, long rowCount);
  }
  
  public static class StdOutSink implements Sink{
    @Override
    public void publishResult(String tableName, String date, long rowCount) {
      System.out.println(tableName+" "+date+" "+rowCount);
    } 
  }
  
  public static class LocalFileSink implements Sink{
    @Override
    public void publishResult(String tableName, String date, long rowCount) {
      BufferedWriter bufferedWriter = null;
      try{
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(("rowcounter_result"))));
        bufferedWriter.write(tableName+" "+date+" "+rowCount);
      }catch(IOException e){
        LOG.error("error when write rowcouonter result into file", e);
      }finally{
        try {
          bufferedWriter.close();
        } catch (IOException e) {
          LOG.error("error when close file", e);
        }
      }
    }
  }

  /**
   * Table name is set in the configuration file, default "table_statistics".
   * Make sure it has column family named "S".
   * The row count will saved as qualifier "RowNum". 
   */
  public static class HBaseSink implements Sink{
    private HTable table = null;
    
    public HBaseSink(){
      String outputTable = conf.get("coprocessor.rowcounter.table", "coprocessor_rowcount");
      try {
        table = new HTable(conf, outputTable);
      } catch (IOException e) {
        LOG.error("error when create HTable", e);
      }
    }
    
    @Override
    public void publishResult(String tableName, String date, long rowCount) {
      final String columnFamily = "S";
      final String columnQualifier = "RowNum";
      if(table != null){
        try {
          Put put = new Put(Bytes.toBytes(tableName+"_"+date));
          put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(String.valueOf(rowCount)));
          table.put(put);
        } catch (IOException e) {
          LOG.error("error when create HTable", e);
        }
        
      }
    }
  }
  
  static int printUsage() {
    System.err.println("Usage: CoprocessorRowcounter <tablename> " +
        "[<column1> <column2>...] [--startrow=] [--stoprow=] [--maxthread=] [--speed=]");
    System.err.println(" startrow     beginning of row");
    System.err.println(" stoprow      end of the row");
    System.err.println(" maxthread    threads for all regions to run concurrently");
    System.err.println(" speed        rows per second for each region");
    return -1;
  }
  
  private void processArgs(String[] args){
    tableName = args[0];
    columnFamilies = new ArrayList<String>();
    for(int i=1; i<args.length; ++i){
      if(args[i].startsWith("--startrow=")){
        startRow = args[i].substring("--startrow=".length());
      }else if(args[i].startsWith("--stoprow=")){
        stopRow = args[i].substring("--stoprow=".length());
      }else if(args[i].startsWith("--maxthread=")){
        conf.setInt("hbase.htable.threads.max", Integer.parseInt(args[i].substring("--maxthread=".length())));
      }else if(args[i].startsWith("--speed=")){
        speed = Integer.parseInt(args[i].substring("--speed=".length()));
      }else{
        columnFamilies.add(args[i]);
      }
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("ERROR: Wrong number of parameters: " + args.length);
      return printUsage();
    }
    processArgs(args);
    conf.setLong("hbase.rpc.timeout", 600000);
    AggregationClient aggregationClient = new AggregationClient(conf);
    Scan scan = new Scan();
    
    if(startRow != null){
      scan.setStartRow(Bytes.toBytes(startRow));
    }
    if(stopRow != null){
      scan.setStopRow(Bytes.toBytes(stopRow));
    }
    for(int i=0; i<columnFamilies.size(); ++i){
      scan.addFamily(Bytes.toBytes(columnFamilies.get(i)));
    }
    
    long rowCount = 0;
    try {
      rowCount = aggregationClient.rowCountWithSpeed(Bytes.toBytes(tableName), null, scan, speed);
    } catch (Throwable e) {
      LOG.error("error when call rowCount in AggregationClient", e);
    }

    SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd");
    String date = dateFormater.format(new Date());
    sink.publishResult(tableName, date, rowCount);
    
    return 0;
  }
  
  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int errCode = ToolRunner.run(new CoprocessorRowcounter(), args);
    System.exit(errCode);
  }
  
}

/*
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.EndPointClient;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteProtocol.DeleteType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Tool for delete columns
 */
public class BulkDeleteTool extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(BulkDeleteTool.class);

  private String tableName = null;
  private List<String> columns = new ArrayList<String>();
  private String startRow = null;
  private String stopRow = null;
  private Long timestamp = null;
  private int batchSize = 1024;

  static int printUsage() {
    System.err
        .println("Usage: BulkDeleteTool <tablename> "
            + "[<column1> <column2>...] [--startrow=] [--stoprow=] [--maxthread=] " 
            + "[--timestamp=] [--batchsize=]");
    System.err.println(" startrow     beginning of row");
    System.err.println(" stoprow      end of the row");
    System.err
        .println(" maxthread    threads for all regions to run concurrently");
    System.err
        .println(" timestamp    delete all kv whose timestamp less than or equal to this timestamp."
            + " Default: current timestamp in server side");
    System.err
        .println(" batchsize    batch size for bulk delete. Default: 1024.");
    System.err
        .println(" eg: BulkDeleteTool t1 f1        delete all data in column famliy f1 of t1");
    System.err
        .println(" eg: BulkDeleteTool t1 f1:c1     delete all data in column f1:c1 of t1");
    return -1;
  }

  private void processArgs(String[] args) {
    this.tableName = args[0];
    for (int i = 1; i < args.length; ++i) {
      if (args[i].startsWith("--startrow=")) {
        startRow = args[i].substring("--startrow=".length());
      } else if (args[i].startsWith("--stoprow=")) {
        stopRow = args[i].substring("--stoprow=".length());
      } else if (args[i].startsWith("--maxthread=")) {
        getConf().setInt("hbase.htable.threads.max",
          Integer.parseInt(args[i].substring("--maxthread=".length())));
      } else if (args[i].startsWith("--timestamp=")) {
        timestamp = Long.parseLong(args[i].substring("--timestamp=".length()));
      } else if (args[i].startsWith("--batchSize=")) {
        batchSize = Integer.parseInt(args[i].substring("--batchSize=".length()));
      } else {
        columns.add(args[i]);
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

    Scan scan = new Scan();
    if (startRow != null) {
      scan.setStartRow(Bytes.toBytes(startRow));
    }
    if (stopRow != null) {
      scan.setStopRow(Bytes.toBytes(stopRow));
    }
    for (String column : columns) {
      int index = column.indexOf(':');
      if (index == -1) {
        scan.addFamily(Bytes.toBytes(column));
      } else {
        byte[] family = Bytes.toBytes(column.substring(0, index));
        byte[] qualifier = Bytes.toBytes(column.substring(index + 1));
        scan.addColumn(family, qualifier);
      }
    }
    HTable ht = new HTable(getConf(), tableName);
    EndPointClient eClient = new EndPointClient();
    try {
      BulkDeleteResponse response =
          eClient.delete(ht, scan, DeleteType.COLUMN, timestamp, batchSize);
      long noOfDeletedRows = response.getRowsDeleted();
      LOG.info("noOfDeletedRows: " + noOfDeletedRows
          + " are deleted in columns: " + Arrays.toString(columns.toArray()));
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {
      ht.close();
    }
    return 0;
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int errCode = ToolRunner.run(HBaseConfiguration.create(), new BulkDeleteTool(), args);
    System.exit(errCode);
  }
}

/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapreduce.fds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.FDSMessageScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

@InterfaceAudience.Private
public class FDSInputSplit extends InputSplit implements Writable {

  private FDSMessageScanner scanner;

  public FDSInputSplit() {
  }

  public FDSInputSplit(FDSMessageScanner scanner) {
    this.scanner = scanner;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, Bytes.toBytes(scanner.getEndpoint()));
    Bytes.writeByteArray(out, Bytes.toBytes(scanner.getBucket()));
    Bytes.writeByteArray(out, Bytes.toBytes(scanner.getPartition()));
    Bytes.writeByteArray(out, Bytes.toBytes(scanner.getCredential().getGalaxyAccessId()));
    Bytes.writeByteArray(out, Bytes.toBytes(scanner.getCredential().getGalaxyAccessSecret()));
    Bytes.writeByteArray(out, Bytes.toBytes(scanner.getTimeRange().getMin()));
    Bytes.writeByteArray(out, Bytes.toBytes(scanner.getTimeRange().getMax()));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    FDSMessageScanner.FDSMessageScannerBuilder builder = FDSMessageScanner.newBuilder();
    String endpoint = Bytes.toString(Bytes.readByteArray(in));
    String bucket = Bytes.toString(Bytes.readByteArray(in));
    String partition = Bytes.toString(Bytes.readByteArray(in));
    String accessKey = Bytes.toString(Bytes.readByteArray(in));
    String accessSecret = Bytes.toString(Bytes.readByteArray(in));
    long startTime = Bytes.toLong(Bytes.readByteArray(in));
    long endTime = Bytes.toLong(Bytes.readByteArray(in));
    builder.withEndpoint(endpoint)
        .withBucketName(bucket)
        .withPartition(partition)
        .withCredential(accessKey, accessSecret)
        .withTimeRange(startTime, endTime);
    this.scanner = builder.build();
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  public FDSMessageScanner getScanner() {
    return scanner;
  }
}

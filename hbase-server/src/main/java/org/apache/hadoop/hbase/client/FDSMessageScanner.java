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
package org.apache.hadoop.hbase.client;

import com.xiaomi.infra.thirdparty.galaxy.fds.client.FDSClientConfiguration;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.GalaxyFDS;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.credential.BasicFDSCredential;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.credential.GalaxyFDSCredential;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.exception.GalaxyFDSClientException;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.model.FDSObject;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.model.FDSObjectSummary;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.TalosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class FDSMessageScanner extends AbstractClientScanner {
  private static final Logger LOG = LoggerFactory.getLogger(FDSMessageScanner.class);

  private final String endpoint;
  private final GalaxyFDSCredential credential;
  private final String bucket;
  private final String fileName;
  private final TimeRange timeRange;
  private final LinkedList<Result> cache = new LinkedList<>();
  private long fileSize;
  private boolean init;
  private DataInputStream in;
  private boolean closed;
  private byte[] buffer;
  private AtomicLong offset;

  private FDSMessageScanner(FDSMessageScannerBuilder builder) {
    this.endpoint = builder.endpoint;
    this.credential = builder.credential;
    this.bucket = builder.bucket;
    this.fileName = builder.fileName;
    this.fileSize = builder.fileSize;

    this.timeRange = builder.timeRange;
    this.buffer = new byte[4096];
    this.offset = new AtomicLong(0);
    this.init = false;
    this.closed = false;
  }

  private void init() throws IOException {
    FDSClientConfiguration fdsConfig = new FDSClientConfiguration(endpoint);
    fdsConfig.enableCdnForDownload(false);
    fdsConfig.enableHttps(true);
    GalaxyFDS fdsClient = new GalaxyFDSClient(credential, fdsConfig);
    try {
      if (!fdsClient.doesBucketExist(bucket)) {
        throw new IOException("bucket doesn't exist... Please check your configuration");
      }
      FDSObject fileObject = fdsClient.getObject(bucket, fileName);
      this.in = new DataInputStream(fileObject.getObjectContent());
      if(fileSize == 0) {
        this.fileSize = fileObject.getObjectSummary().getSize();
      }
      this.init = true;
    } catch (GalaxyFDSClientException e) {
      throw new IOException(e);
    }

  }

  @Override
  public Result next() throws IOException {
    if (!this.init) {
      init();
    }
    if (cache.size() == 0 && this.closed) {
      return null;
    }
    if (cache.size() == 0) {
      loadData();
    }
    if (cache.size() > 0) {
      return cache.poll();
    }
    return null;
  }

  private void loadData() throws IOException {
    boolean shouldContinue = true;
    while (shouldContinue) {
      try {
        Pair<Integer, Message> messageAndReadBytes = TalosUtil.getNextMessageFromStream(in);
        List<Result> results =
            TalosUtil.convertMessageToResult(messageAndReadBytes.getSecond(), timeRange);
        long currentOffset = offset.addAndGet(messageAndReadBytes.getFirst());
        cache.addAll(results);
        if (results.size() > 0) {
          shouldContinue = false;
        }
        if (currentOffset >= fileSize) {
          shouldContinue = false;
          this.close();
        }
      } catch (EOFException eof) {
        LOG.warn(
          "file {} reached to end without a complete message, current offset: {}, fileSize:{}",
          new Object[] { fileName, offset.get(), fileSize }, eof);
        this.close();
        return;
      }
    }
  }

  @Override
  public void close() {
    this.closed = true;
    try {
      in.close();
    } catch (IOException e) {
      LOG.warn("close file failed", e);
      return;
    }
  }

  public String getEndpoint() {
    return endpoint;
  }

  public GalaxyFDSCredential getCredential() {
    return credential;
  }

  public String getBucket() {
    return bucket;
  }

  public String getFileName() {
    return fileName;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  public long getFileSize() {
    return fileSize;
  }

  public long getOffset() {
    return offset.get();
  }

  public static FDSMessageScannerBuilder newBuilder() {
    return new FDSMessageScannerBuilder();
  }

  public static class FDSMessageScannerBuilder {
    private String endpoint;
    private GalaxyFDSCredential credential;
    private String bucket;
    private String fileName;
    private TimeRange timeRange = new TimeRange();
    private long fileSize = 0;

    public FDSMessageScannerBuilder withEndpoint(String endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    public FDSMessageScannerBuilder withCredential(String accessKey, String accessSecret) {
      this.credential = new BasicFDSCredential(accessKey, accessSecret);
      return this;
    }

    public FDSMessageScannerBuilder withBucketName(String bucket) {
      this.bucket = bucket;
      return this;
    }

    public FDSMessageScannerBuilder withTimeRange(long startTime, long endTime) {
      this.timeRange = new TimeRange(startTime, endTime);
      return this;
    }

    public FDSMessageScannerBuilder withFileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    public FDSMessageScannerBuilder withFileSize(long size) {
      this.fileSize = size;
      return this;
    }

    public FDSMessageScannerBuilder withFileObjectSummary(FDSObjectSummary summary) {
      this.fileName = summary.getObjectName();
      this.fileSize = summary.getSize();
      this.bucket = summary.getBucketName();
      return this;
    }

    public FDSMessageScanner build() throws IOException {
      if (credential == null || endpoint == null || bucket == null || fileName == null) {
        throw new IOException("endpoint, credential, bucket and fileName must be set");
      }
      return new FDSMessageScanner(this);
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 7) {
      throw new IllegalArgumentException("usage: ./hbase " + FDSMessageScanner.class.getName()
          + " app_id app_secret endpoint bucket file startTime endTime");
    }
    String APP_ACCESS_KEY = args[0];
    String APP_ACCESS_SECRET = args[1];
    String endpoint = args[2];
    String bucket = args[3];
    String file = args[4];
    long startTime = Long.parseLong(args[5]);
    long endTime = Long.parseLong(args[6]);
    FDSMessageScannerBuilder builder = FDSMessageScanner.newBuilder();
    FDSMessageScanner scanner =
        builder.withTimeRange(startTime, endTime).withBucketName(bucket).withEndpoint(endpoint)
            .withCredential(APP_ACCESS_KEY, APP_ACCESS_SECRET).withFileName(file).build();
    Result result;
    while ((result = scanner.next()) != null) {
      System.out.println("get a result " + result);
    }
    System.out.print("" + scanner.getOffset());
  }
}

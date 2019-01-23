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

import com.xiaomi.infra.thirdparty.galaxy.fds.client.FDSClientConfiguration;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.credential.BasicFDSCredential;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.credential.GalaxyFDSCredential;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.exception.GalaxyFDSClientException;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.model.FDSObjectListing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.FDSMessageScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
public class FDSInputFormat extends InputFormat<ImmutableBytesWritable, Result> {
  private final Logger LOG = LoggerFactory.getLogger(FDSInputFormat.class);

  static final String FDS_BUCKET_NAME = "galaxy.fds.bucket.name";
  static final String FDS_ENDPOINT = "galaxy.fds.endpoint";
  static final String FDS_ACCESS_KEY = "galaxy.fds.access.key";
  static final String FDS_ACCESS_SECRET = "galaxy.fds.access.secret";
  static final String FDS_START_TIME = "galaxy.fds.start.time";
  static final String FDS_STOP_TIME = "galaxy.fds.stop.time";

  private String endpoint;
  private String bucketName;
  private String accessKey;
  private String accessSecret;
  private long startTime;
  private long endTime;

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();
    endpoint = conf.get(FDS_ENDPOINT);
    bucketName = conf.get(FDS_BUCKET_NAME);
    accessKey = conf.get(FDS_ACCESS_KEY);
    accessSecret = conf.get(FDS_ACCESS_SECRET);
    if(endpoint == null || accessKey == null || accessSecret == null || bucketName == null) {
      throw new IOException(
          "please set all configs of endpoint, accessKey, accessSecret and bucketName");
    }

    startTime = conf.getLong(FDS_START_TIME, 0);
    endTime = conf.getLong(FDS_STOP_TIME, Long.MAX_VALUE);

    FDSClientConfiguration fdsConfig = new FDSClientConfiguration(endpoint);
    GalaxyFDSCredential credential = new BasicFDSCredential(accessKey, accessSecret);
    GalaxyFDSClient fdsClient = new GalaxyFDSClient(credential, fdsConfig);

    try {
      if (!fdsClient.doesBucketExist(bucketName)) {
        throw new IOException("this bucket doesn't exist, please check: " + bucketName);
      }
      List<String> partitions = getAllPartitions(fdsClient);
      return createSplits(partitions);
    } catch (GalaxyFDSClientException e) {
      throw new IOException("Get a exception from FDS: ", e);
    }
  }

  private List<InputSplit> createSplits(List<String> partitions) throws IOException {
    List<InputSplit> inputSplits = new ArrayList<>();
    FDSMessageScanner.FDSMessageScannerBuilder builder = FDSMessageScanner.newBuilder();
    builder.withEndpoint(endpoint)
        .withBucketName(bucketName)
        .withTimeRange(startTime, endTime)
        .withCredential(accessKey, accessSecret);
    for(String partition : partitions) {
      LOG.info("add a split for partition {}", partition);
      inputSplits.add(new FDSInputSplit(builder.withPartition(partition).build()));
    }
    return inputSplits;
  }

  private List<String> getAllPartitions(GalaxyFDSClient fdsClient) throws GalaxyFDSClientException {
    FDSObjectListing listing = null;
    List<String> partitions = new ArrayList<>();
    do {
      if (listing == null) {
        listing = fdsClient.listObjects(bucketName, getPrefix(bucketName), "/");
      } else {
        listing = fdsClient.listNextBatchOfObjects(listing);
      }
      if (listing != null) {
        partitions.addAll(listing.getCommonPrefixes());
      }
    } while (listing != null && listing.isTruncated());
    return partitions;
  }

  private String getPrefix(String bucket) {
    return bucket.replaceAll("\\.", "_") + "/";
  }

  @Override
  public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new FDSRecordReader();
  }
}

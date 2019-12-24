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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.TalosUtil;
import org.apache.hadoop.hbase.util.TalosUtil.MessageChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class FDSMessageScanner extends AbstractClientScanner {
  private static final Logger LOG = LoggerFactory.getLogger(FDSMessageScanner.class);
  private static final long DEFAULT_KEEP_ALIVE_TIME = 10 * 60 * 1000; // 10 mins

  private final LinkedList<Result> cache = new LinkedList<>();
  private String endpoint = null;
  private GalaxyFDSCredential credential = null;
  private String bucket = null;
  private String partition = null;
  private TimeRange timeRange = null;
  private GalaxyFDS fdsClient;
  private boolean init;
  private boolean finished;
  private AtomicLong offset;
  private FDSObject currentFile;
  private DataInputStream currentInputStream;
  private List<FDSObjectSummary> fileList;
  private ListIterator<FDSObjectSummary> fileListIterator;
  private LinkedList<MessageChunk> messageChunks;

  @VisibleForTesting
  public FDSMessageScanner(LinkedList<MessageChunk> messageChunkList, DataInputStream inputStream){
    messageChunks = messageChunkList;
    currentInputStream = inputStream;
  }

  private FDSMessageScanner(FDSMessageScannerBuilder builder) {
    this.endpoint = builder.endpoint;
    this.credential = builder.credential;
    this.bucket = builder.bucket;
    this.partition = builder.partition;

    this.timeRange = builder.timeRange;
    this.offset = new AtomicLong(0);
    this.init = false;
    this.finished = false;
  }

  public void init() throws IOException {
    FDSClientConfiguration fdsConfig = new FDSClientConfiguration(endpoint);
    fdsConfig.enableCdnForDownload(false);
    fdsConfig.enableHttps(true);
    fdsConfig.setHTTPKeepAliveTimeoutMS(DEFAULT_KEEP_ALIVE_TIME);
    fdsClient = new GalaxyFDSClient(credential, fdsConfig);
    try {
      if (!fdsClient.doesBucketExist(bucket)) {
        throw new IOException("bucket doesn't exist... Please check your configuration");
      }
      fileList = fdsClient.listObjects(bucket, partition).getObjectSummaries();
      LOG.info("there are {} files under this partition", fileList.size());
      //sort the list
      fileList.sort(Comparator.comparing(FDSObjectSummary::getObjectName));
      fileListIterator = fileList.listIterator();
      messageChunks = new LinkedList<>();
      switchFile();
      this.init = true;
    } catch (GalaxyFDSClientException e) {
      throw new IOException(e);
    }
  }

  public String getPartition() {
    return partition;
  }

  public ListIterator<FDSObjectSummary> getFileListIterator() {
    return fileListIterator;
  }

  public List<FDSObjectSummary> getFileList() {
    return fileList;
  }

  public LinkedList<MessageChunk> getMessageChunks() {
    return messageChunks;
  }

  @Override
  public Result next() throws IOException {
    if (!this.init) {
      init();
    }
    if (cache.size() == 0 && this.finished) {
      return null;
    }
    if (cache.size() == 0) {
      try {
        loadData();
      }catch (GalaxyFDSClientException e){
        LOG.error("failed to read file from FDS", e);
        throw new IOException(e);
      }
    }
    if (cache.size() > 0) {
      return cache.poll();
    }
    return null;
  }

  private void switchFile() throws GalaxyFDSClientException {
    currentInputStream = null;
    while (fileListIterator.hasNext()) {
      FDSObjectSummary candidate = fileListIterator.next();
      if (!timeRange.withinOrAfterTimeRange(candidate.getUploadTime())) {
        LOG.info("skip this file {} because it's stale, upload time: {}", candidate.getObjectName(),
          candidate.getUploadTime());
        continue;
      }
      currentFile = fdsClient.getObject(bucket, candidate.getObjectName());
      currentInputStream = new DataInputStream(currentFile.getObjectContent());
      break;
    }
    if (currentInputStream == null) {
      // there is no more files to read
      this.finished = true;
    }
  }


  private void loadData() throws IOException, GalaxyFDSClientException {
    boolean shouldContinue = true;
    while (shouldContinue) {
      if(finished){
        return;
      }
      if (loadNextMessage()) {
        Message message = TalosUtil.mergeChunksToMessage(messageChunks);
        List<Result> results = TalosUtil.convertMessageToResult(message, timeRange);
        if (results.size() > 0) {
          cache.addAll(results);
          shouldContinue = false;
        }
      }
    }
  }

  @VisibleForTesting
  public boolean loadNextMessage() throws IOException, GalaxyFDSClientException {
    boolean done = false;
    try {
      while (!done) {
        MessageChunk messageChunk = TalosUtil.readMessageChunk(currentInputStream);
        if (shouldClearChunkList(messageChunk)) {
          messageChunks.clear();
        }
        if (isValidChunk(messageChunk)) {
          messageChunks.add(messageChunk);
        }
        if (messageChunk.isLastChunk()) {
          done = messageChunks.size() == messageChunk.getTotalSlices();
        }
      }
    } catch (EOFException eof) {
      LOG.warn("file reached to the end, need to switch next file", eof);
      this.close();
      switchFile();
    }
    return done;
  }

  private boolean shouldClearChunkList(MessageChunk messageChunk) {
    // if chunk list is not empty, but the chunk we read has different seqNum with last Chunk, that
    // means the list is corrupt, should be cleared.
    return !messageChunks.isEmpty()
        && messageChunk.getSeqNum() != messageChunks.getLast().getSeqNum();
  }

  private boolean isValidChunk(MessageChunk messageChunk) {
    // a valid chunk is the first chunk when chunk list is empty, or has a incrementing index
    // compare to last chunk
    return (messageChunks.isEmpty() && messageChunk.isFirstChunk())
        || messageChunk.getIndex() == messageChunks.getLast().getIndex() + 1;
  }

  @Override
  public void close() {
    try {
      if(currentInputStream != null) {
        currentInputStream.close();
      }
    } catch (IOException e) {
      LOG.warn("close current file {} failed", currentFile.getObjectSummary().getObjectName(),  e);
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


  public TimeRange getTimeRange() {
    return timeRange;
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
    private String partition;
    private TimeRange timeRange = new TimeRange();

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

    public FDSMessageScannerBuilder withPartition(String partition) {
      this.partition = partition;
      return this;
    }

    public FDSMessageScanner build() throws IOException {
      if (credential == null || endpoint == null || bucket == null || partition == null) {
        throw new IOException("endpoint, credential, bucket and partition must be set");
      }
      return new FDSMessageScanner(this);
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 7) {
      throw new IllegalArgumentException("usage: ./hbase " + FDSMessageScanner.class.getName()
          + " app_id app_secret endpoint bucket partition startTime endTime");
    }
    String APP_ACCESS_KEY = args[0];
    String APP_ACCESS_SECRET = args[1];
    String endpoint = args[2];
    String bucket = args[3];
    String partition = args[4];
    long startTime = Long.parseLong(args[5]);
    long endTime = Long.parseLong(args[6]);
    FDSMessageScannerBuilder builder = FDSMessageScanner.newBuilder();
    FDSMessageScanner scanner =
        builder.withTimeRange(startTime, endTime).withBucketName(bucket).withEndpoint(endpoint)
            .withCredential(APP_ACCESS_KEY, APP_ACCESS_SECRET).withPartition(partition).build();
    long count = 0;
    while (scanner.next() != null) {
      count++;
      if(count % 10000 == 0){
        System.out.println("already scan " + count);
      }
    }
    System.out.print("" + scanner.getOffset());
  }

  @Override
  public boolean renewLease() {
    throw new UnsupportedOperationException();
  }
}

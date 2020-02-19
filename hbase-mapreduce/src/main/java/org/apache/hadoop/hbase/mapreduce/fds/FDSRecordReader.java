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

import java.io.IOException;

import org.apache.hadoop.hbase.client.FDSMessageScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class FDSRecordReader extends RecordReader<ImmutableBytesWritable, Result> {
  private FDSMessageScanner scanner;
  private ImmutableBytesWritable key;
  private Result value;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    scanner = ((FDSInputSplit) inputSplit).getScanner();
    key = new ImmutableBytesWritable();
    value = new Result();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    value = scanner.next();
    if (value != null && value.size() > 0) {
      key.set(value.getRow());
      return true;
    }
    return false;
  }

  @Override
  public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Result getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    int fileCount = scanner.getFileList().size();
    if(fileCount <= 0) {
      return 0;
    }
    int index = scanner.getFileListIterator().previousIndex();
    return  (index == -1 ? 0 : index) / (float)fileCount;
  }

  @Override
  public void close() throws IOException {
    scanner.close();
  }
}

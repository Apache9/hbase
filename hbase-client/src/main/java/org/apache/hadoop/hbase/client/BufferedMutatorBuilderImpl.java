package org.apache.hadoop.hbase.client;

import java.util.concurrent.TimeUnit;

public class BufferedMutatorBuilderImpl implements BufferedMutatorBuilder {

  private AsyncBufferedMutatorBuilder asyncBuilder;

  public BufferedMutatorBuilderImpl(AsyncBufferedMutatorBuilder asyncBuilder) {
    this.asyncBuilder = asyncBuilder;
  }

  @Override
  public BufferedMutatorBuilder setOperationTimeout(long timeout, TimeUnit unit) {
    asyncBuilder.setOperationTimeout(timeout, unit);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setRpcTimeout(long timeout, TimeUnit unit) {
    asyncBuilder.setRpcTimeout(timeout, unit);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setRetryPause(long pause, TimeUnit unit) {
    asyncBuilder.setRetryPause(pause, unit);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setMaxAttempts(int maxAttempts) {
    asyncBuilder.setMaxAttempts(maxAttempts);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setStartLogErrorsCnt(int startLogErrorsCnt) {
    asyncBuilder.setStartLogErrorsCnt(startLogErrorsCnt);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setWriteBufferSize(long writeBufferSize) {
    asyncBuilder.setWriteBufferSize(writeBufferSize);
    return this;
  }

  @Override
  public BufferedMutator build() {
    return new BufferedMutatorImpl(asyncBuilder.build());
  }
}

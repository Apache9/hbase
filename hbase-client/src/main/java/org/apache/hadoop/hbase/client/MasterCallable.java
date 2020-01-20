package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.HIGH_QOS;
import static org.apache.hadoop.hbase.HConstants.NORMAL_QOS;
import static org.apache.hadoop.hbase.HRegionInfo.FIRST_META_REGIONINFO;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Parent of {@link MasterCallable} and {@link MasterCallable}. Has common methods.
 * @param <V>
 */
@InterfaceAudience.Private
abstract class MasterCallable<V> implements RetryingCallable<V>, Closeable {

  private final HConnection connection;

  private MasterKeepAliveConnection master;

  private final HBaseRpcController controller;

  private final int priority;

  private MasterCallable(HConnection connection, boolean isSystemTable) {
    this.connection = connection;
    this.controller = connection.getRpcControllerFactory().newController();
    this.priority = isSystemTable ? HIGH_QOS : NORMAL_QOS;
  }

  public MasterCallable(HConnection connection) {
    this(connection, false);
  }

  public MasterCallable(HConnection connection, TableName tableName) {
    this(connection, tableName.isSystemTable());
  }

  public MasterCallable(HConnection connection, byte[] regionName) {
    this(connection, Bytes.equals(regionName, FIRST_META_REGIONINFO.getRegionName()) ||
        Bytes.equals(regionName, FIRST_META_REGIONINFO.getEncodedNameAsBytes()));
  }

  @Override
  public void prepare(int callTimeout, boolean reload) throws IOException {
    this.master = this.connection.getKeepAliveMasterService();
  }

  @Override
  public void whenRegionNotServing() throws IOException {
  }

  @Override
  public void close() throws IOException {
    // The above prepare could fail but this would still be called though masterAdmin is null
    if (this.master != null) {
      this.master.close();
    }
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return "";
  }

  @Override
  public long sleep(long pause, int tries) {
    return ConnectionUtils.getPauseTime(pause, tries);
  }

  @Override
  public V call(int callTimeout) throws Exception {
    controller.reset();
    controller.setCallTimeout(callTimeout);
    controller.setPriority(priority);
    return rpcCall(master, controller);
  }

  protected abstract V rpcCall(MasterService.BlockingInterface master,
      HBaseRpcController controller) throws Exception;
}
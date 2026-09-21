/**
 * 
 */
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.BooleanStateStore;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * Store whether rpc throttle is enabled.
 * <p>
 * Notice that, this is stored in master local region so only master can read it. For region
 * servers, we will publish the change through
 * {@link org.apache.hadoop.hbase.master.procedure.SwitchRpcThrottleProcedure}, and we will also
 * include this in region server start up responses, and region servers can also query the flag
 * through rpc request to master.
 */
@InterfaceAudience.Private
public class RpcThrottleStateStore extends BooleanStateStore {

  public static final String STATE_NAME = "rpc-throttle";

  protected RpcThrottleStateStore(MasterRegion masterRegion, String stateName, ZKWatcher watcher,
    String zkPath) throws IOException, KeeperException, DeserializationException {
    super(masterRegion, stateName, watcher, zkPath);
  }

  @Override
  protected byte[] toByteArray(boolean on) {
    return Bytes.toBytes(on);
  }

  @Override
  protected boolean parseFrom(byte[] bytes) throws DeserializationException {
    return Bytes.toBoolean(bytes);
  }

}

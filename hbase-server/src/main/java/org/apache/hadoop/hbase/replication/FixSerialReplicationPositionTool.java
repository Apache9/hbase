package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FixSerialReplicationPositionTool extends Configured implements Tool {
  private static final Log LOG =
      LogFactory.getLog(FixSerialReplicationPositionTool.class);
  private HConnection connection;

  public FixSerialReplicationPositionTool(Configuration conf) throws IOException {
    connection = HConnectionManager.createConnection(conf);
  }

  /**
   * The guts of the {@link #main} method.
   * Call this method to avoid the {@link #main(String[])} System.exit.
   * @param args
   * @return errCode
   * @throws Exception
   */
  static int innerMain(final Configuration conf, final String [] args) throws Exception {
    return ToolRunner.run(conf, new FixSerialReplicationPositionTool(conf), args);
  }

  public static void main(String[] args) throws Exception {
    System.exit(innerMain(HBaseConfiguration.create(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    Map<String, List<Long>> barrierMap = MetaEditor.getAllBarriers(connection);
    for (Map.Entry<String, List<Long>> entry : barrierMap.entrySet()) {
      String encodedName = entry.getKey();
      List<Long> barriers = entry.getValue();
      byte[] encodedBytes = Bytes.toBytes(encodedName);
      Map<String, Long> posMap = MetaEditor.getReplicationPositionForAllPeer(
          connection, encodedBytes);
      for (Map.Entry<String, Long> posEntry : posMap.entrySet()) {
        String peer = posEntry.getKey();
        long pos = posEntry.getValue();
        for (int i = 0; i < barriers.size(); i++) {
          if (pos < barriers.get(i)) {
            long newPos = barriers.get(i) - 1;
            if (pos != newPos) {
              LOG.info(
                  "Found a mismatch pos, need fix region=" + encodedName + " peer=" + peer
                      + " from " + pos + " to " + newPos);
              Map<String, Long> map = new HashMap<>();
              map.put(encodedName, newPos);
              MetaEditor.updateReplicationPositions(connection, peer, map);
            }
            break;
          }
        }
      }
    }
    return 0;
  }
}

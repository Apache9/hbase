package org.apache.hadoop.hbase.master.cleaner;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A cleaner that cleans replication locks on zk which is locked by dead region servers
 */
public class ReplicationZKLockCleanerChore extends Chore {
  private static final Log LOG = LogFactory.getLog(ReplicationZKLockCleanerChore.class);
  private ReplicationZookeeper zookeeper;

  @VisibleForTesting
  // Wait some times before delete lock to prevent a session expired RS not dead fully.
  public static long TTL = 60 * 10 * 1000;//10 min

  public ReplicationZKLockCleanerChore(int p, Stoppable stopper,
      ReplicationZookeeper zk) {
    super("ReplicationZKLockCleanerChore", p, stopper);
    this.zookeeper = zk;
  }

  @Override protected void chore() {
    try {
      List<String> regionServers = zookeeper.getRegisteredRegionServers();
      if (regionServers == null) {
        return;
      }
      Set<String> rsSet = new HashSet<String>(regionServers);
      List<String> replicators = zookeeper.getListOfReplicators();
      for (String replicator: replicators) {
        try {
          String lockNode = ZKUtil.joinZNode(zookeeper.getRsZNode(replicator),
              ReplicationZookeeper.RS_LOCK_ZNODE);
          byte[] data = ZKUtil.getData(zookeeper.getZookeeperWatcher(), lockNode);
          if (data == null) {
            continue;
          }
          String rsServerNameZnode = Bytes.toString(data);
          String[] array = rsServerNameZnode.split("/");
          String znode = array[array.length - 1];
          if (!rsSet.contains(znode)) {
            Stat s =
                zookeeper.getZookeeperWatcher().getRecoverableZooKeeper().exists(lockNode, false);
            if (s != null && System.currentTimeMillis() - s.getMtime() > TTL) {
              // server is dead, but lock is still there, we have to delete the lock.
              ZKUtil.deleteNode(zookeeper.getZookeeperWatcher(), lockNode);
              LOG.info("Remove lock acquired by dead RS: " + lockNode + " by " + znode);
            }
            continue;
          }
          LOG.info("Skip lock acquired by live RS: " + lockNode + " by " + znode);

        } catch (KeeperException.NoNodeException ignore) {
        } catch (InterruptedException e) {
          LOG.warn("zk operation interrupted", e);
        }
      }
    } catch (KeeperException e) {
      LOG.warn("zk operation interrupted", e);
    }

  }
}

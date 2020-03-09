package org.apache.hadoop.hbase.replication;

import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_ENDPOINT;
import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_KEY;
import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_SECRET;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.replication.HBaseStreamReplicationEndpoint.TableStreamConfig;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestHBaseStreamReplicationEndpoint {

  HBaseStreamReplicationEndpoint endpoint;

  @Before
  public void init() throws IOException {
    endpoint = new HBaseStreamReplicationEndpoint();
    Configuration conf = new Configuration();
    conf.set(HConstants.CLUSTER_NAME, "test-stream");
    ReplicationPeerConfig peerConfig = new ReplicationPeerConfig();
    peerConfig.getConfiguration().put(TALOS_ACCESS_ENDPOINT, "endpoint");
    peerConfig.getConfiguration().put(TALOS_ACCESS_KEY, "access_key");
    peerConfig.getConfiguration().put(TALOS_ACCESS_SECRET, "access_secret");
    ReplicationPeer peer = Mockito.mock(ReplicationPeer.class);
    Mockito.when(peer.getPeerState()).thenReturn(ReplicationPeer.PeerState.ENABLED);
    Mockito.when(peer.getPeerConfig()).thenReturn(peerConfig);
    MetricsSource ms = Mockito.mock(MetricsSource.class);
    Mockito.doNothing().when(ms).setAgeOfLastShippedOp(Mockito.anyLong());
    endpoint.ctx =
            new ReplicationEndpoint.Context(conf, conf, null, null, UUID.randomUUID(), peer, ms);
    endpoint.init(endpoint.ctx);
  }

  @Test
  public void testTableTalosStreamConfig() {
    // config string is null
    TableStreamConfig streamConfig =
        new TableStreamConfig(null);
    Assert.assertEquals(HBaseStreamReplicationEndpoint.FieldsControl.MUTATE_LOG, streamConfig.getFieldsControl());
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.Put));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.Delete));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.DeleteColumn));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.DeleteFamily));
    // config string is empty string
    streamConfig = new TableStreamConfig("");
    Assert.assertEquals(HBaseStreamReplicationEndpoint.FieldsControl.MUTATE_LOG, streamConfig.getFieldsControl());
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.Put));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.Delete));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.DeleteColumn));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.DeleteFamily));
    Assert.assertEquals(HBaseStreamReplicationEndpoint.FieldsControl.MUTATE_LOG, streamConfig.getFieldsControl());
    // config string is empty json
    streamConfig = new TableStreamConfig("{  }");
    Assert.assertEquals(HBaseStreamReplicationEndpoint.FieldsControl.MUTATE_LOG, streamConfig.getFieldsControl());
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.Put));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.Delete));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.DeleteColumn));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.DeleteFamily));
    Assert.assertEquals(HBaseStreamReplicationEndpoint.FieldsControl.MUTATE_LOG, streamConfig.getFieldsControl());
    // fields_control = MUTATE_LOG
    streamConfig = new TableStreamConfig("{\"fields_control\":\"MUTATE_LOG\"}");
    Assert.assertEquals(HBaseStreamReplicationEndpoint.FieldsControl.MUTATE_LOG, streamConfig.getFieldsControl());
    // fields_control = KEYS_ONLY
    streamConfig = new TableStreamConfig("{\"fields_control\":\"KEYS_ONLY\"}");
    Assert.assertEquals(HBaseStreamReplicationEndpoint.FieldsControl.KEYS_ONLY, streamConfig.getFieldsControl());
    // ops = []
    streamConfig = new TableStreamConfig("{\"ops\":[]}");
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.Put));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.Delete));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.DeleteColumn));
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.DeleteFamily));
    // ops = [Put]
    streamConfig = new TableStreamConfig("{\"ops\":[\"Put\"]}");
    Assert.assertTrue(streamConfig.canAcceptOp(KeyValue.Type.Put));
    Assert.assertFalse(streamConfig.canAcceptOp(KeyValue.Type.Delete));
  }

}

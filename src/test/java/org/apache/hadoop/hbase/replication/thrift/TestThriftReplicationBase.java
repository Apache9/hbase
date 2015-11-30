package org.apache.hadoop.hbase.replication.thrift;

import org.junit.BeforeClass;

public class TestThriftReplicationBase {

  @BeforeClass
  public static void setUpKlazz() throws Exception {
    // Error level to skip some warnings specific to the minicluster. See HBASE-4709
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.util.MBeans.class).setLevel( org.apache.log4j.Level.ERROR);
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class).setLevel(org.apache.log4j.Level.ERROR);

  }
}

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicLongMap;

public class AccessCounter extends Chore {
  private final static Log LOG = LogFactory.getLog(AccessCounter.class);
  
  public static final String FLUSH_ACCESS_COUNTER_PERIOD = "hbase.server.flush.access.counter.period";
  public static final int DEFAULT_FLUSH_ACCESS_COUNTER_PERIOD_MS = 600000;
  
  public static final byte[] ACCOUNT_TABLE_NAME = Bytes.toBytes("_access_account_");
  public static final byte[] READ_FAMILY = Bytes.toBytes("R");
  public static final byte[] WRITE_FAMILY = Bytes.toBytes("W");
  
  public static final String KEY_DELIMITER = "#";
  public static final byte[] COLUMN_SEPARATOR = Bytes.toBytes(",");
  
  private HRegionServer server;
  private AtomicLongMap<CounterKey> counter;
  private HTableInterface table = null;
  private Configuration conf;
  private boolean enabled = false;
  private long timestamp;
  private int period;
  
  public AccessCounter(final HRegionServer server, int period) {
    super("AccessCounter", period, server);
    this.server = server;
    this.conf = server.getConfiguration();
    this.period = period;
    counter = AtomicLongMap.create();
    timestamp = normalizeTimestamp();
    LOG.info("New AccessCounter : flush period=" + period);
  }
  
  private boolean init() {
    try {
      if (isCounterEnabled()) {
        return true;
      }
      if (MetaReader.tableExists(server.getCatalogTracker(), Bytes.toString(ACCOUNT_TABLE_NAME))) {
        if (table == null) {
          table = new HTable(this.conf, ACCOUNT_TABLE_NAME);
        }
        enabled = true;
        LOG.info("Enable AccessCounter by init table " + Bytes.toString(ACCOUNT_TABLE_NAME));
        return true;
      }
    } catch (IOException e) {
      enabled = false;
      LOG.warn("There was error when init table " + Bytes.toString(ACCOUNT_TABLE_NAME) + ", " + e);
    }
    return false;
  }

  public boolean isCounterEnabled() {
    return enabled;
  }

  @Override
  protected void chore() {
    if (server.isStopped()) {
      return;
    }
    if (!init()) {
      return;
    }
    if (table == null) {
      return;
    }
    LOG.info("Start to flush counter data to table " + Bytes.toString(ACCOUNT_TABLE_NAME));
    long nextTimestamp = normalizeTimestamp();
    for (Entry<CounterKey, Long> entry : counter.asMap().entrySet()) {
      CounterKey key = entry.getKey();
      long currentValue = entry.getValue();
      Increment increment = makeIncrement(key, currentValue);
      try {
        table.increment(increment);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Make a increment " + increment + " to " + Bytes.toString(ACCOUNT_TABLE_NAME));
        }
      } catch (Exception e) {
        LOG.warn("Fail to increment " + increment + " to " + Bytes.toString(ACCOUNT_TABLE_NAME));
        continue;
      }
      counter.addAndGet(key, 0 - currentValue);
    }
    timestamp = nextTimestamp;
  }
  
  @VisibleForTesting
  public long normalizeTimestamp() {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    return now - (now % period);
  }

  private Increment makeIncrement(CounterKey key, long value) {
    Increment increment = new Increment(key.getRowKey(timestamp));
    if (key.getType().equals(AccessType.READ)) {
      increment.addColumn(READ_FAMILY, key.getColumn(), value);
    } else {
      increment.addColumn(WRITE_FAMILY, key.getColumn(), value);
    }
    return increment;
  }
  
  protected void incrementReadCount(User user, byte[] tableName, byte[] family, byte[] qualifier) {
    if (!isCounterEnabled()) {
      return;
    }
    CounterKey key = new CounterKey(user.getShortName(), tableName, family, qualifier, AccessType.READ);
    counter.incrementAndGet(key);
  }
  
  protected void incrementWriteCount(User user, byte[] tableName, byte[] family, byte[] qualifier) {
    if (!isCounterEnabled()) {
      return;
    }
    CounterKey key = new CounterKey(user.getShortName(), tableName, family, qualifier, AccessType.WRITE);
    counter.incrementAndGet(key);
  }

  protected void addReadCount(CounterKey key, long delta) {
    if (!isCounterEnabled()) {
      return;
    }
    counter.addAndGet(key, delta);
  }

  protected void addWriteCount(CounterKey key, long delta) {
    if (!isCounterEnabled()) {
      return;
    }
    counter.addAndGet(key, delta);
  }

  @VisibleForTesting
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  
  public enum AccessType {
    READ,
    WRITE
  }
  
  public static class CounterKey {
    private String userName;
    private byte[] tableName;
    private byte[] column;
    private AccessType type;
    
    public CounterKey(String userName, byte[] tableName, byte[] family, byte[] qualifier, AccessType type) {
      this.userName = userName;
      this.tableName = tableName;
      this.column = Bytes.add(family, COLUMN_SEPARATOR, qualifier);
      this.type = type;
    }
    
    public byte[] getRowKey(long timestamp) {
      return Bytes.add(Bytes.toBytes(userName + KEY_DELIMITER), tableName, Bytes
          .toBytes(KEY_DELIMITER + new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss").format(timestamp)));
    }
    
    public String getUserName() {
      return userName;
    }

    public void setUserName(String userName) {
      this.userName = userName;
    }

    public byte[] getTableName() {
      return tableName;
    }

    public void setTableName(byte[] tableName) {
      this.tableName = tableName;
    }

    public byte[] getColumn() {
      return column;
    }

    public void setColumn(byte[] column) {
      this.column = column;
    }

    public AccessType getType() {
      return type;
    }

    public void setType(AccessType type) {
      this.type = type;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(column);
      result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      result = prime * result + ((userName == null) ? 0 : userName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      CounterKey other = (CounterKey) obj;
      if (!Arrays.equals(column, other.column)) return false;
      if (tableName == null) {
        if (other.tableName != null) return false;
      } else if (!tableName.equals(other.tableName)) return false;
      if (type != other.type) return false;
      if (userName == null) {
        if (other.userName != null) return false;
      } else if (!userName.equals(other.userName)) return false;
      return true;
    }
  }
}

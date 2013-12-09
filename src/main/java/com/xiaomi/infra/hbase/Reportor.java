package com.xiaomi.infra.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HRegionInfo;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class Reportor extends Configured {
  private static final Log LOG = LogFactory.getLog(Reportor.class);

  class Availability {
    private boolean access = true;
    private int allRegion = 0;
    private int failedRegion = 0;

    public Availability() {
      this(true, 0, 0);
    }

    public Availability(boolean access, int allRegion, int failedRegion) {
      this.access = access;
      this.allRegion = allRegion;
      this.failedRegion = failedRegion;
    }

    public synchronized void addSample(boolean success) {
      this.allRegion++;
      if (!success) {
        this.failedRegion++;
      }
    }

    public double getAvailability() {
      if (this.access == false) return 0.0;
      if (this.allRegion == 0) return 0.0;
      return 1.0 - 1.0 * this.failedRegion / this.allRegion;
    }
  }

  private ConcurrentHashMap<String, Availability> tableMap = new ConcurrentHashMap<String, Availability>();

  public void reportAvailable(HRegionInfo region, boolean available) {
    String tableName = region.getTableNameAsString();
    if (!tableMap.containsKey(tableName)) {
      tableMap.putIfAbsent(tableName, new Availability());
    }
    tableMap.get(tableName).addSample(available);
  }

  public double GetAvailability(final String tableName) {
    if (this.tableMap.containsKey(tableName)) {
      return this.tableMap.get(tableName).getAvailability();
    }
    return 0.0;
  }

  public void summary(Agent agent) throws IOException {
    for (Entry<String, Availability> entry : tableMap.entrySet()) {
      LOG.info("Table: " + entry.getKey() + " Availability: "
          + entry.getValue().getAvailability() * 100 + " %");
      agent.write(entry.getKey() + "-Availability", "", entry.getValue()
          .getAvailability() * 100);
    }
  }

  public void reset() {
    this.tableMap.clear();
  }
  
  public void failedTable(String table) {
    tableMap.put(table, new Availability(false, 0, 0));
  }
}
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.util.Strings;

/**
 * This class is used exporting current statistics of filesystem.
 */
public class FileSystemStatistics {
  private long oldLogsFileCount;
  private long oldLogsSpaceConsumed;

  public FileSystemStatistics(long oldLogsFileCount, long oldLogsSpaceConsumed) {
    this.oldLogsFileCount = oldLogsFileCount;
    this.oldLogsSpaceConsumed = oldLogsSpaceConsumed;
  }

  public long getOldLogsSpaceConsumed() {
    return oldLogsSpaceConsumed;
  }

  public long getOldLogsFileCount() {
    return oldLogsFileCount;
  }

  @Override
  public String toString() {
    StringBuilder sb =
        Strings.appendKeyValue(new StringBuilder(), "oldLogsFileCount", this.oldLogsFileCount);
    sb = Strings.appendKeyValue(sb, "oldLogsSpaceConsumed", this.oldLogsSpaceConsumed);
    return sb.toString();
  }
}

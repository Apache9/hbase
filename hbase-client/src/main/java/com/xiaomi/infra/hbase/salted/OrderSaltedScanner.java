package com.xiaomi.infra.hbase.salted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class OrderSaltedScanner extends BaseSaltedScanner {
  private List<ResultScanner> iters = new ArrayList<ResultScanner>();
  private int currentScannerIdx = 0;
  
  public OrderSaltedScanner(Scan[] scans, HTableInterface table) throws IOException {
    for (Scan scan : scans) {
      iters.add(table.getScanner(scan));
    }
  }

  @Override
  public Result next() throws IOException {
    if (this.closed)
      return null;
    while (currentScannerIdx < iters.size()) {
      Result result = iters.get(currentScannerIdx).next();
      if (result == null) {
        ++currentScannerIdx;
        continue;
      }
      return result;
    }
    return null;
  }

  @Override
  public void close() {
    if (this.closed) {
      return;
    }
    for (ResultScanner iter : iters) {
      iter.close();
    }
    this.closed = true;
  }
}
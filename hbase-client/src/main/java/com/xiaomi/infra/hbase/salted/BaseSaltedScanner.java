package com.xiaomi.infra.hbase.salted;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;

public abstract class BaseSaltedScanner extends AbstractClientScanner {
  protected boolean closed = false;
  
  @Override
  public Result[] next(int nbRows) throws IOException {
    ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
    for(int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[resultSets.size()]);
  }
}

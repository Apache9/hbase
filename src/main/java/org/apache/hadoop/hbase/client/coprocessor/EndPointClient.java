package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteProtocol;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteResponse;

/**
 * This client class is for invoking the functions deployed on the Region Server side via the
 * Endpoint Protocols, such as BulkDeleteProtocol etc.
 */
public class EndPointClient {
  public BulkDeleteResponse delete(HTableInterface table, final Scan scan, final byte deleteType,
      final Long timestamp, final int rowBatchSize) throws Throwable {
    class BulkDeleteCallBack implements Batch.Callback<BulkDeleteResponse> {
      BulkDeleteResponse response = null;

      BulkDeleteResponse getResponse() {
        return response;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, BulkDeleteResponse result) {
        if (response == null) {
          response = new BulkDeleteResponse();
          response.setRowsDeleted(0);
          response.setVersionsDeleted(0);
        }
        response.setRowsDeleted(response.getRowsDeleted() + result.getRowsDeleted());
        response.setVersionsDeleted(response.getVersionsDeleted() + result.getVersionsDeleted());
      }
    }
    
    BulkDeleteCallBack callback = new BulkDeleteCallBack();
    table.coprocessorExec(BulkDeleteProtocol.class, scan.getStartRow(), scan.getStopRow(),
      new Batch.Call<BulkDeleteProtocol, BulkDeleteResponse>() {
        @Override
        public BulkDeleteResponse call(BulkDeleteProtocol instance) throws IOException {
          BulkDeleteResponse response = instance.delete(scan, deleteType, timestamp, rowBatchSize);
          if (response.getIoException() != null) {
            throw response.getIoException();
          }
          return response;
        }
      }, callback);
    return callback.getResponse();
  }
}

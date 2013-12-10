package org.apache.hadoop.hbase.coprocessor.example;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestBulkDeleteResponse {
  public void checkBulkDeleteResponseEq(BulkDeleteResponse expected, BulkDeleteResponse actual) {
    Assert.assertEquals(expected.getRowsDeleted(), actual.getRowsDeleted());
    Assert.assertEquals(expected.getVersionsDeleted(), actual.getVersionsDeleted());
    if (expected.getIoException() == null) {
      Assert.assertNull(actual.getIoException());
    } else {
      Assert.assertEquals(expected.getIoException().getClass().getName(), expected.getIoException()
          .getClass().getName());
      Assert.assertEquals(StringUtils.stringifyException(expected.getIoException()),
        StringUtils.stringifyException(expected.getIoException()));
    }
  }
  
  @Test
  public void writeAndReadFieldOfBulkDeleteResponse() throws IOException {
    // without read/write ioException
    BulkDeleteResponse expected = new BulkDeleteResponse();
    expected.setRowsDeleted(1);
    expected.setVersionsDeleted(2);
    DataOutputBuffer os = new DataOutputBuffer();
    expected.write(os);
    BulkDeleteResponse actual = new BulkDeleteResponse();
    actual.readFields(new DataInputStream(new ByteArrayInputStream(os.getData())));
    checkBulkDeleteResponseEq(expected, actual);
    
    // read/write ioException
    expected.setIoException(new IOException("test_expction"));
    os = new DataOutputBuffer();
    expected.write(os);
    actual = new BulkDeleteResponse();
    actual.readFields(new DataInputStream(new ByteArrayInputStream(os.getData())));
    checkBulkDeleteResponseEq(expected, actual);
  }
}

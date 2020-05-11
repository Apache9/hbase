package org.apache.hadoop.hbase.mapreduce;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;

/**
 * reducer for {@link HBaseStreamCopyTableToTalos} UT
 */
@InterfaceAudience.Private
public class DummyHBaseStreamCopyTableReducer extends HBaseStreamCopyTableToTalosReducer {

  private static final Logger LOG = LoggerFactory.getLogger(DummyHBaseStreamCopyTableReducer.class);

  public static final String HBASE_STREAM_COPY_TO_TALOS_UT_OUTPUT_FILE =
      "hbase.stream.copytotable.ut.output.file";

  private String outputFile;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    outputFile = conf.get(HBASE_STREAM_COPY_TO_TALOS_UT_OUTPUT_FILE);
  }

  @Override
  protected void productMessages(Cell cell, List<Message> messages) throws IOException {
    FileOutputStream writer = new FileOutputStream(outputFile, true);
    // write messages bytes into file
    for (Message message : messages) {
      writer.write(message.getMessage());
      writer.write("\n".getBytes());
      writer.flush();
    }
  }
}

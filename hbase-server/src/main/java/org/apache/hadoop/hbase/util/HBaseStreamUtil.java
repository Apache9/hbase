package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.util.TalosUtil.CHUNK_SIZE;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class HBaseStreamUtil {

  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
  private static final String TOPIC_NAME_PREFIX = "hbase-stream-";

  public static List<Message> toMessage(JsonElement json) {
    byte[] messageBytes = json.toString().getBytes(UTF8_CHARSET);

    List<Message> messages = new ArrayList<>();
    int totalSlices = (messageBytes.length + CHUNK_SIZE - 1) / CHUNK_SIZE;
    for (int index = 0; index < totalSlices; index++) {
      int offset = index * CHUNK_SIZE;
      int length = (offset + CHUNK_SIZE) > messageBytes.length ? (messageBytes.length - offset) : CHUNK_SIZE;
      byte[] messageSlice = Bytes.copy(messageBytes, offset, length);
      byte[] header = Bytes.add(Bytes.toBytes(index), Bytes.toBytes(totalSlices));
      Message message = new Message(ByteBuffer.wrap(Bytes.add(header, messageSlice)));
      messages.add(message);
    }
    return messages;
  }

  public static JsonObject cellToJson(Cell cell, String value) {
    String rowkeyInBase64 = HBaseStreamUtil.toBase64String(CellUtil.cloneRow(cell));
    String family = Bytes.toString(CellUtil.cloneFamily(cell));
    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
    long timestamp = cell.getTimestamp();
    JsonObject jsonKvObj = new JsonObject();
    jsonKvObj.addProperty("r", rowkeyInBase64);
    jsonKvObj.addProperty("op", KeyValue.Type.codeToType(cell.getTypeByte()).name());
    jsonKvObj.addProperty("t", timestamp);
    if (family != null) {
      jsonKvObj.addProperty("f", family);
    }
    if (qualifier != null && !"".equals(qualifier)) {
      jsonKvObj.addProperty("q", qualifier);
    }
    if (value != null && !"".equals(value)) {
      jsonKvObj.addProperty("v", value);
    }
    return jsonKvObj;
  }

  public static String encodeTopicName(String tableName) {
    return TOPIC_NAME_PREFIX + TalosUtil.encodeTableName(tableName);
  }

  public static String toBase64String(byte[] bytes) {
    return null == bytes ? null : Base64.getEncoder().encodeToString(bytes);
  }

}

package org.apache.hadoop.hbase.ipc;

import java.net.InetSocketAddress;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Throw this in rpc call if there are too many pending requests for one region server
 */
public class ServerBusyException extends DoNotRetryIOException {

  public ServerBusyException(InetSocketAddress address, long count){
    super("There are "+count+" concurrent rpc requests for "+address);
  }

}

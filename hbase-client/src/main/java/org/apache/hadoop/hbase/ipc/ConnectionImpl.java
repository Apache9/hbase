package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.TextFormat;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.SaslException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.ipc.FailedServers.FailedServer;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.security.HBaseSaslRpcClient;
import org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Thread that reads responses and notifies callers. Each connection owns a socket connected to a
 * remote address. Calls are multiplexed through this socket: responses may be delivered out of
 * order.
 */
@InterfaceAudience.Private
class ConnectionImpl extends Connection implements Runnable {

  private static final Log LOG = LogFactory.getLog(ConnectionImpl.class);

  private final RpcClientImpl rpcClient;
  final Thread thread;

  protected Socket socket = null; // connected socket
  protected DataInputStream in;
  protected DataOutputStream out;

  private HBaseSaslRpcClient saslRpcClient;

  // currently active calls
  protected final ConcurrentSkipListMap<Integer, Call> calls = new ConcurrentSkipListMap<Integer, Call>();
  protected final AtomicLong lastActivity = new AtomicLong(); // last I/O activity time
  protected final AtomicBoolean shouldCloseConnection = new AtomicBoolean(); // indicate if the
                                                                             // connection is
                                                                             // closed
  protected IOException closeException; // close reason

  ConnectionImpl(RpcClientImpl rpcClient, ConnectionId remoteId, Codec codec,
      CompressionCodec compressor) throws IOException {
    super(rpcClient.conf, remoteId, rpcClient.clusterId,
        rpcClient.userProvider.isHBaseSecurityEnabled(), rpcClient.pingInterval, codec, compressor);
    this.rpcClient = rpcClient;

    this.thread = new Thread(this);
    UserGroupInformation ticket = remoteId.getTicket().getUGI();
    thread.setName("IPC Client (" + rpcClient.socketFactory.hashCode() + ") connection to "
        + remoteId.getAddress().toString()
        + ((ticket == null) ? " from an unknown user" : (" from " + ticket.getUserName())));
    thread.setDaemon(true);
  }

  /** Update lastActivity with the current time. */
  protected void touch() {
    lastActivity.set(System.currentTimeMillis());
  }

  /**
   * Add a call to this connection's call queue and notify a listener; synchronized. If the
   * connection is dead, the call is not added, and the caller is notified. This function can return
   * a connection that is already marked as 'shouldCloseConnection' It is up to the user code to
   * check this status.
   * @param call to add
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY", justification = "Notify because new call available for processing")
  protected synchronized void addCall(Call call) {
    // If the connection is about to close, we manage this as if the call was already added
    // to the connection calls list. If not, the connection creations are serialized, as
    // mentioned in HBASE-6364
    if (this.shouldCloseConnection.get()) {
      if (this.closeException == null) {
        call.setException(new IOException(
            "Call " + call.id + " not added as the connection " + remoteId + " is closing"));
      } else {
        call.setException(this.closeException);
      }
      synchronized (call) {
        call.notifyAll();
      }
    } else {
      calls.put(call.id, call);
      synchronized (call) {
        notify();
      }
    }
  }

  /**
   * This class sends a ping to the remote side when timeout on reading. If no failure is detected,
   * it retries until at least a byte is read.
   */
  protected class PingInputStream extends FilterInputStream {
    /* constructor */
    protected PingInputStream(InputStream in) {
      super(in);
    }

    /*
     * Process timeout exception if the connection is not going to be closed, send a ping.
     * otherwise, throw the timeout exception.
     */
    private void handleTimeout(SocketTimeoutException e) throws IOException {
      if (shouldCloseConnection.get() || !ConnectionImpl.this.rpcClient.running.get()
          || remoteId.rpcTimeout > 0) {
        throw e;
      }
      sendPing();
    }

    /**
     * Read a byte from the stream. Send a ping if timeout on read. Retries if no failure is
     * detected until a byte is read.
     * @throws IOException for any IO problem other than socket timeout
     */
    @Override
    public int read() throws IOException {
      do {
        try {
          return super.read();
        } catch (SocketTimeoutException e) {
          handleTimeout(e);
        }
      } while (true);
    }

    /**
     * Read bytes into a buffer starting from offset <code>off</code> Send a ping if timeout on
     * read. Retries if no failure is detected until a byte is read.
     * @return the total number of bytes read; -1 if the connection is closed.
     */
    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
      do {
        try {
          return super.read(buf, off, len);
        } catch (SocketTimeoutException e) {
          handleTimeout(e);
        }
      } while (true);
    }
  }

  protected synchronized void setupConnection() throws IOException {
    short ioFailures = 0;
    short timeoutFailures = 0;
    while (true) {
      try {
        this.socket = this.rpcClient.socketFactory.createSocket();
        this.socket.setTcpNoDelay(this.rpcClient.tcpNoDelay);
        this.socket.setKeepAlive(this.rpcClient.tcpKeepAlive);
        if (this.rpcClient.localAddr != null) {
          this.socket.bind(this.rpcClient.localAddr);
        }
        // connection time out is 20s
        NetUtils.connect(this.socket, remoteId.getAddress(),
          RpcClientImpl.getSocketTimeout(this.rpcClient.conf));
        this.socket.setSoTimeout(pingInterval);
        return;
      } catch (SocketTimeoutException toe) {
        /*
         * The max number of retries is 45, which amounts to 20s*45 = 15 minutes retries.
         */
        handleConnectionFailure(timeoutFailures++, this.rpcClient.maxRetries, toe);
      } catch (IOException ie) {
        handleConnectionFailure(ioFailures++, this.rpcClient.maxRetries, ie);
      }
    }
  }

  @Override
  public void shutdown() {
    if (socket == null) {
      return;
    }

    // close the current connection
    try {
      if (socket.getOutputStream() != null) {
        socket.getOutputStream().close();
      }
    } catch (IOException ignored) { // Can happen if the socket is already closed
    }
    try {
      if (socket.getInputStream() != null) {
        socket.getInputStream().close();
      }
    } catch (IOException ignored) { // Can happen if the socket is already closed
    }
    try {
      if (socket.getChannel() != null) {
        socket.getChannel().close();
      }
    } catch (IOException ignored) { // Can happen if the socket is already closed
    }
    try {
      socket.close();
    } catch (IOException e) {
      LOG.warn("Not able to close a socket", e);
    }

    // set socket to null so that the next call to setupIOstreams
    // can start the process of connect all over again.
    socket = null;
  }

  /**
   * Handle connection failures If the current number of retries is equal to the max number of
   * retries, stop retrying and throw the exception; Otherwise backoff N seconds and try connecting
   * again. This Method is only called from inside setupIOstreams(), which is synchronized. Hence
   * the sleep is synchronized; the locks will be retained.
   * @param curRetries current number of retries
   * @param maxRetries max number of retries allowed
   * @param ioe failure reason
   * @throws IOException if max number of retries is reached
   */
  private void handleConnectionFailure(int curRetries, int maxRetries, IOException ioe)
      throws IOException {
    shutdown();

    // throw the exception if the maximum number of retries is reached
    if (curRetries >= maxRetries || ExceptionUtil.isInterrupt(ioe)) {
      throw ioe;
    }

    // otherwise back off and retry
    try {
      LOG.info(
        "Sleep in handleConnectionFailure for next try, sleepTime=" + this.rpcClient.failureSleep);
      Thread.sleep(this.rpcClient.failureSleep);
    } catch (InterruptedException ie) {
      ExceptionUtil.rethrowIfInterrupt(ie);
    }

    LOG.info("Retrying connect to server: " + remoteId.getAddress() + " after sleeping "
        + this.rpcClient.failureSleep + "ms. Already tried " + curRetries + " time(s).");
  }

  /*
   * wait till someone signals us to start reading RPC response or it is idle too long, it is marked
   * as to be closed, or the client is marked as not running. Return true if it is time to read a
   * response; false otherwise.
   */
  protected synchronized boolean waitForWork() {
    if (calls.isEmpty() && !shouldCloseConnection.get() && this.rpcClient.running.get()) {
      long timeout = this.rpcClient.maxIdleTime - (System.currentTimeMillis() - lastActivity.get());
      if (timeout > 0) {
        try {
          wait(timeout);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }

    if (!calls.isEmpty() && !shouldCloseConnection.get() && this.rpcClient.running.get()) {
      return true;
    } else if (shouldCloseConnection.get()) {
      return false;
    } else if (calls.isEmpty()) { // idle connection closed or stopped
      markClosed(null);
      return false;
    } else { // get stopped but there are still pending requests
      markClosed((IOException) new IOException().initCause(new InterruptedException()));
      return false;
    }
  }

  public InetSocketAddress getRemoteAddress() {
    return remoteId.getAddress();
  }

  /*
   * Send a ping to the server if the time elapsed since last I/O activity is equal to or greater
   * than the ping interval
   */
  protected synchronized void sendPing() throws IOException {
    // Can we do tcp keepalive instead of this pinging?
    long curTime = System.currentTimeMillis();
    if (curTime - lastActivity.get() >= pingInterval) {
      lastActivity.set(curTime);
      // noinspection SynchronizeOnNonFinalField
      synchronized (this.out) {
        out.writeInt(RpcClientImpl.PING_CALL_ID);
        out.flush();
      }
    }
  }

  @Override
  public void run() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("starting, connections " + this.rpcClient.connections.size());
    }

    try {
      while (waitForWork()) { // Wait here for work - read or close connection
        readResponse();
      }
    } catch (Throwable t) {
      LOG.warn("unexpected exception receiving call responses", t);
      markClosed(new IOException("Unexpected exception receiving call responses", t));
    }

    close();

    if (LOG.isDebugEnabled()) {
      LOG.debug("stopped, connections " + this.rpcClient.connections.size());
    }
  }

  private synchronized void disposeSasl() {
    if (saslRpcClient != null) {
      saslRpcClient.dispose();
      saslRpcClient = null;
    }
  }

  private synchronized boolean setupSaslConnection(final InputStream in2, final OutputStream out2)
      throws IOException {
    saslRpcClient = new HBaseSaslRpcClient(authMethod, token, serverPrincipal,
        this.rpcClient.fallbackAllowed, this.rpcClient.conf.get("hbase.rpc.protection",
          QualityOfProtection.AUTHENTICATION.name().toLowerCase()));
    return saslRpcClient.saslConnect(in2, out2);
  }

  /**
   * If multiple clients with the same principal try to connect to the same server at the same time,
   * the server assumes a replay attack is in progress. This is a feature of kerberos. In order to
   * work around this, what is done is that the client backs off randomly and tries to initiate the
   * connection again. The other problem is to do with ticket expiry. To handle that, a relogin is
   * attempted.
   * <p>
   * The retry logic is governed by the {@link #shouldAuthenticateOverKrb} method. In case when the
   * user doesn't have valid credentials, we don't need to retry (from cache or ticket). In such
   * cases, it is prudent to throw a runtime exception when we receive a SaslException from the
   * underlying authentication implementation, so there is no retry from other high level (for eg,
   * HCM or HBaseAdmin).
   * </p>
   */
  private synchronized void handleSaslConnectionFailure(final int currRetries, final int maxRetries,
      final Exception ex, final Random rand, final UserGroupInformation user)
      throws IOException, InterruptedException {
    user.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException, InterruptedException {
        shutdown();
        if (shouldRelogin(ex)) {
          if (currRetries < maxRetries) {
            LOG.debug("Exception encountered while connecting to " + "the server : ", ex);
            // try re-login
            relogin();
            disposeSasl();
            // have granularity of milliseconds
            // we are sleeping with the Connection lock held but since this
            // connection instance is being used for connecting to the server
            // in question, it is okay
            Thread.sleep((rand.nextInt(reloginMaxBackoff) + 1));
            return null;
          } else {
            String msg = "Couldn't setup connection for "
                + UserGroupInformation.getLoginUser().getUserName() + " to " + serverPrincipal;
            LOG.warn(msg);
            throw (IOException) new IOException(msg).initCause(ex);
          }
        } else {
          LOG.warn("Exception encountered while connecting to " + "the server : ", ex);
        }
        if (ex instanceof RemoteException) {
          throw (RemoteException) ex;
        }
        if (ex instanceof SaslException) {
          String msg = "SASL authentication failed."
              + " The most likely cause is missing or invalid credentials." + " Consider 'kinit'.";
          LOG.fatal(msg, ex);
          throw new RuntimeException(msg, ex);
        }
        throw new IOException(ex);
      }
    });
  }

  private synchronized void connect() throws IOException {
    if (socket != null || shouldCloseConnection.get()) {
      return;
    }

    FailedServer failedServer = this.rpcClient.failedServers.getFailedServer(remoteId.getAddress());
    if (failedServer != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not trying to connect to " + remoteId.address
            + " this server is in the failed servers list");
      }
      IOException e = new FailedServerException("This server is in the failed servers list"
          + failedServer.formatFailedTimeStamp() + ": " + remoteId.address, failedServer.cause);
      markClosed(e);
      close();
      throw e;
    }

    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to " + remoteId.address);
      }
      short numRetries = 0;
      final short MAX_RETRIES = 5;
      Random rand = null;
      while (true) {
        setupConnection();
        InputStream inStream = NetUtils.getInputStream(socket);
        // This creates a socket with a write timeout. This timeout cannot be changed,
        // RpcClient allows to change the timeout dynamically, but we can only
        // change the read timeout today.
        OutputStream outStream = NetUtils.getOutputStream(socket, pingInterval);
        // Write out the preamble -- MAGIC, version, and auth to use.
        writeConnectionHeaderPreamble(outStream);
        if (useSasl) {
          final InputStream in2 = inStream;
          final OutputStream out2 = outStream;
          UserGroupInformation ticket = getUGI();
          boolean continueSasl = false;
          if (ticket == null) {
            throw new FatalConnectionException("ticket/user is null");
          }
          try {
            continueSasl = ticket.doAs(new PrivilegedExceptionAction<Boolean>() {
              @Override
              public Boolean run() throws IOException {
                return setupSaslConnection(in2, out2);
              }
            });
          } catch (Exception ex) {
            if (rand == null) {
              rand = new Random();
            }
            handleSaslConnectionFailure(numRetries++, MAX_RETRIES, ex, rand, ticket);
            continue;
          }
          if (continueSasl) {
            // Sasl connect is successful. Let's set up Sasl i/o streams.
            inStream = saslRpcClient.getInputStream(inStream);
            outStream = saslRpcClient.getOutputStream(outStream);
          } else {
            // fall back to simple auth because server told us so.
            // do not change authMethod and useSasl here, we should start from secure when
            // reconnecting because regionserver may change its sasl config after restart.
          }
        }
        this.in = new DataInputStream(new BufferedInputStream(new PingInputStream(inStream)));
        this.out = new DataOutputStream(new BufferedOutputStream(outStream));
        // Now write out the connection header
        writeConnectionHeader();

        // update last activity time
        touch();

        // start the receiver thread after the socket connection has been set up
        thread.start();
        return;
      }
    } catch (Throwable t) {
      this.rpcClient.failedServers.addToFailedServers(remoteId.address, t);
      IOException e = null;
      if (t instanceof LinkageError) {
        // probably the hbase hadoop version does not match the running hadoop version
        e = new DoNotRetryIOException(t);
        markClosed(e);
      } else if (t instanceof IOException) {
        e = (IOException) t;
        markClosed(e);
      } else {
        e = new IOException("Could not set up IO Streams", t);
        markClosed(e);
      }
      close();
      throw e;
    }
  }

  /**
   * Write the RPC header: <MAGIC WORD -- 'HBas'> <ONEBYTE_VERSION> <ONEBYTE_AUTH_TYPE>
   */
  private void writeConnectionHeaderPreamble(OutputStream outStream) throws IOException {
    outStream.write(getConnectionHeaderPreamble());
    outStream.flush();
  }

  /**
   * Write the connection header. Out is not synchronized because only the first thread does this.
   */
  private void writeConnectionHeader() throws IOException {
    synchronized (this.out) {
      this.out.writeInt(this.header.getSerializedSize());
      this.header.writeTo(this.out);
      this.out.flush();
    }
  }

  /** Close the connection. */
  protected synchronized void close() {
    if (!shouldCloseConnection.get()) {
      LOG.error(thread.getName() + ": the connection is not in the closed state");
      return;
    }

    // release the resources
    // first thing to do;take the connection out of the connection list
    synchronized (this.rpcClient.connections) {
      this.rpcClient.connections.removeValue(remoteId, this);
    }

    // close the streams and therefore the socket
    if (this.out != null) {
      synchronized (this.out) {
        IOUtils.closeStream(out);
        this.out = null;
      }
    }
    IOUtils.closeStream(in);
    this.in = null;
    disposeSasl();

    // clean up all calls
    if (closeException == null) {
      if (!calls.isEmpty()) {
        LOG.warn(thread.getName() + ": connection is closed for no cause and calls are not empty. "
            + "#Calls: " + calls.size());

        // clean up calls anyway
        closeException = new IOException("Unexpected closed connection");
        cleanupCalls();
      }
    } else {
      // log the info
      if (LOG.isDebugEnabled()) {
        LOG.debug(thread.getName() + ": closing ipc connection to " + remoteId.address + ": "
            + closeException.getMessage(),
          closeException);
      }

      // cleanup calls
      cleanupCalls();
    }
    if (LOG.isDebugEnabled()) LOG.debug(thread.getName() + ": closed");
  }

  /**
   * Initiates a call by sending the parameter to the remote server. Note: this is not called from
   * the Connection thread, but by other threads.
   * @param call
   * @param priority
   * @see #readResponse()
   */
  protected void writeRequest(Call call) {
    if (shouldCloseConnection.get()) return;
    try {
      ByteBuffer cellBlock = this.rpcClient.ipcUtil.buildCellBlock(this.codec, this.compressor,
        call.cells);
      CellBlockMeta cellBlockMeta;
      if (cellBlock != null) {
        CellBlockMeta.Builder cellBlockMetaBuilder = CellBlockMeta.newBuilder();
        cellBlockMetaBuilder.setLength(cellBlock.limit());
        cellBlockMeta = cellBlockMetaBuilder.build();
      } else {
        cellBlockMeta = null;
      }
      RequestHeader requestHeader = IPCUtil.buildRequestHeader(call, cellBlockMeta);
      // noinspection SynchronizeOnNonFinalField
      synchronized (this.out) { // FindBugs IS2_INCONSISTENT_SYNC
        IPCUtil.write(this.out, requestHeader, call.param, cellBlock);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          thread.getName() + ": wrote request header " + TextFormat.shortDebugString(header));
      }
    } catch (IOException e) {
      synchronized (this) {
        if (!shouldCloseConnection.get()) {
          markClosed(e);
          thread.interrupt();
        }
      }
    }
  }

  /*
   * Receive a response. Because only one receiver, so no synchronization on in.
   */
  protected void readResponse() {
    if (shouldCloseConnection.get()) return;
    touch();
    int totalSize = -1;
    try {
      // See HBaseServer.Call.setResponse for where we write out the response.
      // Total size of the response. Unused. But have to read it in anyways.
      totalSize = in.readInt();

      // Read the header
      ResponseHeader responseHeader = ResponseHeader.parseDelimitedFrom(in);
      int id = responseHeader.getCallId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("got response header " + TextFormat.shortDebugString(responseHeader)
            + ", totalSize: " + totalSize + " bytes");
      }
      Call call = calls.get(id);
      if (call == null) {
        // So we got a response for which we have no corresponding 'call' here on the client-side.
        // We probably timed out waiting, cleaned up all references, and now the server decides
        // to return a response. There is nothing we can do w/ the response at this stage. Clean
        // out the wire of the response so its out of the way and we can get other responses on
        // this connection.
        int readSoFar = IPCUtil.getTotalSizeWhenWrittenDelimited(responseHeader);
        int whatIsLeftToRead = totalSize - readSoFar;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unknown callId: " + id + ", skipping over this response of " + whatIsLeftToRead
              + " bytes");
        }
        IOUtils.skipFully(in, whatIsLeftToRead);
      }
      if (responseHeader.hasException()) {
        ExceptionResponse exceptionResponse = responseHeader.getException();
        RemoteException re = IPCUtil.createRemoteException(exceptionResponse);
        if (IPCUtil.isFatalConnectionException(exceptionResponse)) {
          markClosed(re);
        } else {
          if (call != null) {
            call.setException(re);
          }
        }
      } else {
        Message value = null;
        // Call may be null because it may have timedout and been cleaned up on this side already
        if (call != null && call.responseDefaultType != null) {
          Builder builder = call.responseDefaultType.newBuilderForType();
          builder.mergeDelimitedFrom(in);
          value = builder.build();
        }
        CellScanner cellBlockScanner = null;
        if (responseHeader.hasCellBlockMeta()) {
          int size = responseHeader.getCellBlockMeta().getLength();
          byte[] cellBlock = new byte[size];
          IOUtils.readFully(this.in, cellBlock, 0, cellBlock.length);
          cellBlockScanner = this.rpcClient.ipcUtil.createCellScanner(this.codec, this.compressor,
            cellBlock);
        }
        // it's possible that this call may have been cleaned up due to a RPC
        // timeout, so check if it still exists before setting the value.
        if (call != null) call.setResponse(value, cellBlockScanner);
      }
      if (call != null) calls.remove(id);
    } catch (IOException e) {
      if (e instanceof SocketTimeoutException && remoteId.rpcTimeout > 0) {
        // Clean up open calls but don't treat this as a fatal condition,
        // since we expect certain responses to not make it by the specified
        // {@link ConnectionId#rpcTimeout}.
        closeException = e;
      } else {
        // Treat this as a fatal condition and close this connection
        markClosed(e);
      }
    } finally {
      if (remoteId.rpcTimeout > 0) {
        cleanupCalls(remoteId.rpcTimeout);
      }
    }
  }

  protected synchronized void markClosed(IOException e) {
    if (shouldCloseConnection.compareAndSet(false, true)) {
      closeException = e;
      notifyAll();
    }
  }

  /* Cleanup all calls and mark them as done */
  protected void cleanupCalls() {
    cleanupCalls(0);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NN_NAKED_NOTIFY", justification = "Notify because timedout")
  protected void cleanupCalls(long rpcTimeout) {
    Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
    while (itor.hasNext()) {
      Call c = itor.next().getValue();
      long waitTime = System.currentTimeMillis() - c.getStartTime();
      if (waitTime >= rpcTimeout) {
        if (this.closeException == null) {
          // There may be no exception in the case that there are many calls
          // being multiplexed over this connection and these are succeeding
          // fine while this Call object is taking a long time to finish
          // over on the server; e.g. I just asked the regionserver to bulk
          // open 3k regions or its a big fat multiput into a heavily-loaded
          // server (Perhaps this only happens at the extremes?)
          this.closeException = new CallTimeoutException(
              "Call id=" + c.id + ", waitTime=" + waitTime + ", rpcTimetout=" + rpcTimeout);
        }
        c.setException(this.closeException);
        synchronized (c) {
          c.notifyAll();
        }
        itor.remove();
      } else {
        break;
      }
    }
    try {
      if (!calls.isEmpty()) {
        Call firstCall = calls.get(calls.firstKey());
        long maxWaitTime = System.currentTimeMillis() - firstCall.getStartTime();
        if (maxWaitTime < rpcTimeout) {
          rpcTimeout -= maxWaitTime;
        }
      }
      if (!shouldCloseConnection.get()) {
        closeException = null;
        RpcClientImpl.setSocketTimeout(socket, (int) rpcTimeout);
      }
    } catch (SocketException e) {
      LOG.debug("Couldn't lower timeout, which may result in longer than expected calls");
    }
  }

  @Override
  public void sendRequest(Call call) throws IOException {
    connect();
    addCall(call);
    writeRequest(call);
  }
}
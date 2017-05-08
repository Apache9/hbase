/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.ByteBufferPool;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslDigestCallbackHandler;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteInput;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedInputStream;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.TextFormat;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.htrace.TraceInfo;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;

/**
 * An RPC server that hosts protobuf described Services.
 *
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public abstract class RpcServer implements RpcServerInterface,
    ConfigurationObserver {
  // LOG is being used in CallRunner and the log level is being changed in tests
  public static final Log LOG = LogFactory.getLog(RpcServer.class);
  protected static final CallQueueTooBigException CALL_QUEUE_TOO_BIG_EXCEPTION
      = new CallQueueTooBigException();

  private final boolean authorize;
  protected boolean isSecurityEnabled;

  public static final byte CURRENT_VERSION = 0;

  /**
   * Whether we allow a fallback to SIMPLE auth for insecure clients when security is enabled.
   */
  public static final String FALLBACK_TO_INSECURE_CLIENT_AUTH =
          "hbase.ipc.server.fallback-to-simple-auth-allowed";

  /**
   * How many calls/handler are allowed in the queue.
   */
  protected static final int DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER = 10;

  protected final CellBlockBuilder cellBlockBuilder;

  protected static final String AUTH_FAILED_FOR = "Auth failed for ";
  protected static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  protected static final Log AUDITLOG = LogFactory.getLog("SecurityLogger."
      + Server.class.getName());
  protected SecretManager<TokenIdentifier> secretManager;
  protected ServiceAuthorizationManager authManager;

  /** This is set to Call object before Handler invokes an RPC and ybdie
   * after the call returns.
   */
  protected static final ThreadLocal<RpcCall> CurCall = new ThreadLocal<>();

  /** Keeps MonitoredRPCHandler per handler thread. */
  protected static final ThreadLocal<MonitoredRPCHandler> MONITORED_RPC = new ThreadLocal<>();

  protected final InetSocketAddress bindAddress;

  protected MetricsHBaseServer metrics;

  protected final Configuration conf;

  /**
   * Maximum size in bytes of the currently queued and running Calls. If a new Call puts us over
   * this size, then we will reject the call (after parsing it though). It will go back to the
   * client and client will retry. Set this size with "hbase.ipc.server.max.callqueue.size". The
   * call queue size gets incremented after we parse a call and before we add it to the queue of
   * calls for the scheduler to use. It get decremented after we have 'run' the Call. The current
   * size is kept in {@link #callQueueSizeInBytes}.
   * @see #callQueueSizeInBytes
   * @see #DEFAULT_MAX_CALLQUEUE_SIZE
   */
  protected final long maxQueueSizeInBytes;
  protected static final int DEFAULT_MAX_CALLQUEUE_SIZE = 1024 * 1024 * 1024;

  /**
   * This is a running count of the size in bytes of all outstanding calls whether currently
   * executing or queued waiting to be run.
   */
  protected final LongAdder callQueueSizeInBytes = new LongAdder();

  protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives

  /**
   * This flag is used to indicate to sub threads when they should go down.  When we call
   * {@link #start()}, all threads started will consult this flag on whether they should
   * keep going.  It is set to false when {@link #stop()} is called.
   */
  volatile boolean running = true;

  /**
   * This flag is set to true after all threads are up and 'running' and the server is then opened
   * for business by the call to {@link #start()}.
   */
  volatile boolean started = false;

  protected AuthenticationTokenSecretManager authTokenSecretMgr = null;

  protected HBaseRPCErrorHandler errorHandler = null;

  protected static final String MAX_REQUEST_SIZE = "hbase.ipc.max.request.size";
  protected static final RequestTooBigException REQUEST_TOO_BIG_EXCEPTION =
      new RequestTooBigException();

  protected static final String WARN_RESPONSE_TIME = "hbase.ipc.warn.response.time";
  protected static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";

  /**
   * Minimum allowable timeout (in milliseconds) in rpc request's header. This
   * configuration exists to prevent the rpc service regarding this request as timeout immediately.
   */
  protected static final String MIN_CLIENT_REQUEST_TIMEOUT = "hbase.ipc.min.client.request.timeout";
  protected static final int DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT = 20;

  /** Default value for above params */
  protected static final int DEFAULT_MAX_REQUEST_SIZE = DEFAULT_MAX_CALLQUEUE_SIZE / 4; // 256M
  protected static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  protected static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  protected final int maxRequestSize;
  protected final int warnResponseTime;
  protected final int warnResponseSize;

  protected final int minClientRequestTimeout;

  protected final Server server;
  protected final List<BlockingServiceAndInterface> services;

  protected final RpcScheduler scheduler;

  protected UserProvider userProvider;

  protected final ByteBufferPool reservoir;
  // The requests and response will use buffers from ByteBufferPool, when the size of the
  // request/response is at least this size.
  // We make this to be 1/6th of the pool buffer size.
  protected final int minSizeForReservoirUse;

  protected volatile boolean allowFallbackToSimpleAuth;

  /**
   * Used to get details for scan with a scanner_id<br/>
   * TODO try to figure out a better way and remove reference from regionserver package later.
   */
  private RSRpcServices rsRpcServices;

  @FunctionalInterface
  protected static interface CallCleanup {
    void run();
  }

  /** Reads calls from a connection and queues them for handling. */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="VO_VOLATILE_INCREMENT",
      justification="False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
  public abstract class Connection implements Closeable {
    // If initial preamble with version and magic has been read or not.
    protected boolean connectionPreambleRead = false;
    // If the connection header has been read or not.
    protected boolean connectionHeaderRead = false;

    protected CallCleanup callCleanup;

    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    protected String hostAddress;
    protected int remotePort;
    protected InetAddress addr;
    protected ConnectionHeader connectionHeader;

    /**
     * Codec the client asked use.
     */
    protected Codec codec;
    /**
     * Compression codec the client asked us use.
     */
    protected CompressionCodec compressionCodec;
    protected BlockingService service;

    protected AuthMethod authMethod;
    protected boolean saslContextEstablished;
    protected boolean skipInitialSaslHandshake;
    private ByteBuffer unwrappedData;
    // When is this set? FindBugs wants to know! Says NP
    private ByteBuffer unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
    protected boolean useSasl;
    protected SaslServer saslServer;
    protected CryptoAES cryptoAES;
    protected boolean useWrap = false;
    protected boolean useCryptoAesWrap = false;
    // Fake 'call' for failed authorization response
    protected static final int AUTHORIZATION_FAILED_CALLID = -1;
    protected ServerCall authFailedCall;
    protected ByteArrayOutputStream authFailedResponse =
        new ByteArrayOutputStream();
    // Fake 'call' for SASL context setup
    protected static final int SASL_CALLID = -33;
    protected ServerCall saslCall;
    // Fake 'call' for connection header response
    protected static final int CONNECTION_HEADER_RESPONSE_CALLID = -34;
    protected ServerCall setConnectionHeaderResponseCall;

    // was authentication allowed with a fallback to simple auth
    protected boolean authenticatedWithFallback;

    protected boolean retryImmediatelySupported = false;

    public UserGroupInformation attemptingUser = null; // user name before auth
    protected User user = null;
    protected UserGroupInformation ugi = null;

    public Connection() {
      this.callCleanup = null;
    }

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public InetAddress getHostInetAddress() {
      return addr;
    }

    public int getRemotePort() {
      return remotePort;
    }

    public VersionInfo getVersionInfo() {
      if (connectionHeader.hasVersionInfo()) {
        return connectionHeader.getVersionInfo();
      }
      return null;
    }

    protected String getFatalConnectionString(final int version, final byte authByte) {
      return "serverVersion=" + CURRENT_VERSION +
      ", clientVersion=" + version + ", authMethod=" + authByte +
      ", authSupported=" + (authMethod != null) + " from " + toString();
    }

    protected UserGroupInformation getAuthorizedUgi(String authorizedId)
        throws IOException {
      UserGroupInformation authorizedUgi;
      if (authMethod == AuthMethod.DIGEST) {
        TokenIdentifier tokenId = HBaseSaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        authorizedUgi = tokenId.getUser();
        if (authorizedUgi == null) {
          throw new AccessDeniedException(
              "Can't retrieve username from tokenIdentifier.");
        }
        authorizedUgi.addTokenIdentifier(tokenId);
      } else {
        authorizedUgi = UserGroupInformation.createRemoteUser(authorizedId);
      }
      authorizedUgi.setAuthenticationMethod(authMethod.authenticationMethod.getAuthMethod());
      return authorizedUgi;
    }

    /**
     * Set up cell block codecs
     * @throws FatalConnectionException
     */
    protected void setupCellBlockCodecs(final ConnectionHeader header)
        throws FatalConnectionException {
      // TODO: Plug in other supported decoders.
      if (!header.hasCellBlockCodecClass()) return;
      String className = header.getCellBlockCodecClass();
      if (className == null || className.length() == 0) return;
      try {
        this.codec = (Codec)Class.forName(className).newInstance();
      } catch (Exception e) {
        throw new UnsupportedCellCodecException(className, e);
      }
      if (!header.hasCellBlockCompressorClass()) return;
      className = header.getCellBlockCompressorClass();
      try {
        this.compressionCodec = (CompressionCodec)Class.forName(className).newInstance();
      } catch (Exception e) {
        throw new UnsupportedCompressionCodecException(className, e);
      }
    }

    /**
     * Set up cipher for rpc encryption with Apache Commons Crypto
     *
     * @throws FatalConnectionException
     */
    protected void setupCryptoCipher(final ConnectionHeader header,
        RPCProtos.ConnectionHeaderResponse.Builder chrBuilder)
        throws FatalConnectionException {
      // If simple auth, return
      if (saslServer == null) return;
      // check if rpc encryption with Crypto AES
      String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
      boolean isEncryption = SaslUtil.QualityOfProtection.PRIVACY
          .getSaslQop().equalsIgnoreCase(qop);
      boolean isCryptoAesEncryption = isEncryption && conf.getBoolean(
          "hbase.rpc.crypto.encryption.aes.enabled", false);
      if (!isCryptoAesEncryption) return;
      if (!header.hasRpcCryptoCipherTransformation()) return;
      String transformation = header.getRpcCryptoCipherTransformation();
      if (transformation == null || transformation.length() == 0) return;
       // Negotiates AES based on complete saslServer.
       // The Crypto metadata need to be encrypted and send to client.
      Properties properties = new Properties();
      // the property for SecureRandomFactory
      properties.setProperty(CryptoRandomFactory.CLASSES_KEY,
          conf.get("hbase.crypto.sasl.encryption.aes.crypto.random",
              "org.apache.commons.crypto.random.JavaCryptoRandom"));
      // the property for cipher class
      properties.setProperty(CryptoCipherFactory.CLASSES_KEY,
          conf.get("hbase.rpc.crypto.encryption.aes.cipher.class",
              "org.apache.commons.crypto.cipher.JceCipher"));

      int cipherKeyBits = conf.getInt(
          "hbase.rpc.crypto.encryption.aes.cipher.keySizeBits", 128);
      // generate key and iv
      if (cipherKeyBits % 8 != 0) {
        throw new IllegalArgumentException("The AES cipher key size in bits" +
            " should be a multiple of byte");
      }
      int len = cipherKeyBits / 8;
      byte[] inKey = new byte[len];
      byte[] outKey = new byte[len];
      byte[] inIv = new byte[len];
      byte[] outIv = new byte[len];

      try {
        // generate the cipher meta data with SecureRandom
        CryptoRandom secureRandom = CryptoRandomFactory.getCryptoRandom(properties);
        secureRandom.nextBytes(inKey);
        secureRandom.nextBytes(outKey);
        secureRandom.nextBytes(inIv);
        secureRandom.nextBytes(outIv);

        // create CryptoAES for server
        cryptoAES = new CryptoAES(transformation, properties,
            inKey, outKey, inIv, outIv);
        // create SaslCipherMeta and send to client,
        //  for client, the [inKey, outKey], [inIv, outIv] should be reversed
        RPCProtos.CryptoCipherMeta.Builder ccmBuilder = RPCProtos.CryptoCipherMeta.newBuilder();
        ccmBuilder.setTransformation(transformation);
        ccmBuilder.setInIv(getByteString(outIv));
        ccmBuilder.setInKey(getByteString(outKey));
        ccmBuilder.setOutIv(getByteString(inIv));
        ccmBuilder.setOutKey(getByteString(inKey));
        chrBuilder.setCryptoCipherMeta(ccmBuilder);
        useCryptoAesWrap = true;
      } catch (GeneralSecurityException | IOException ex) {
        throw new UnsupportedCryptoException(ex.getMessage(), ex);
      }
    }

    private ByteString getByteString(byte[] bytes) {
      // return singleton to reduce object allocation
      return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
    }

    protected UserGroupInformation createUser(ConnectionHeader head) {
      UserGroupInformation ugi = null;

      if (!head.hasUserInfo()) {
        return null;
      }
      UserInformation userInfoProto = head.getUserInfo();
      String effectiveUser = null;
      if (userInfoProto.hasEffectiveUser()) {
        effectiveUser = userInfoProto.getEffectiveUser();
      }
      String realUser = null;
      if (userInfoProto.hasRealUser()) {
        realUser = userInfoProto.getRealUser();
      }
      if (effectiveUser != null) {
        if (realUser != null) {
          UserGroupInformation realUserUgi =
              UserGroupInformation.createRemoteUser(realUser);
          ugi = UserGroupInformation.createProxyUser(effectiveUser, realUserUgi);
        } else {
          ugi = UserGroupInformation.createRemoteUser(effectiveUser);
        }
      }
      return ugi;
    }

    protected void disposeSasl() {
      if (saslServer != null) {
        try {
          saslServer.dispose();
          saslServer = null;
        } catch (SaslException ignored) {
          // Ignored. This is being disposed of anyway.
        }
      }
    }

    /**
     * No protobuf encoding of raw sasl messages
     */
    protected void doRawSaslReply(SaslStatus status, Writable rv,
        String errorClass, String error) throws IOException {
      ByteBufferOutputStream saslResponse = null;
      DataOutputStream out = null;
      try {
        // In my testing, have noticed that sasl messages are usually
        // in the ballpark of 100-200. That's why the initial capacity is 256.
        saslResponse = new ByteBufferOutputStream(256);
        out = new DataOutputStream(saslResponse);
        out.writeInt(status.state); // write status
        if (status == SaslStatus.SUCCESS) {
          rv.write(out);
        } else {
          WritableUtils.writeString(out, errorClass);
          WritableUtils.writeString(out, error);
        }
        saslCall.setSaslTokenResponse(saslResponse.getByteBuffer());
        saslCall.sendResponseIfReady();
      } finally {
        if (saslResponse != null) {
          saslResponse.close();
        }
        if (out != null) {
          out.close();
        }
      }
    }

    public void saslReadAndProcess(ByteBuff saslToken) throws IOException,
        InterruptedException {
      if (saslContextEstablished) {
        if (LOG.isTraceEnabled())
          LOG.trace("Have read input token of size " + saslToken.limit()
              + " for processing by saslServer.unwrap()");

        if (!useWrap) {
          processOneRpc(saslToken);
        } else {
          byte[] b = saslToken.hasArray() ? saslToken.array() : saslToken.toBytes();
          byte [] plaintextData;
          if (useCryptoAesWrap) {
            // unwrap with CryptoAES
            plaintextData = cryptoAES.unwrap(b, 0, b.length);
          } else {
            plaintextData = saslServer.unwrap(b, 0, b.length);
          }
          processUnwrappedData(plaintextData);
        }
      } else {
        byte[] replyToken;
        try {
          if (saslServer == null) {
            switch (authMethod) {
            case DIGEST:
              if (secretManager == null) {
                throw new AccessDeniedException(
                    "Server is not configured to do DIGEST authentication.");
              }
              saslServer = Sasl.createSaslServer(AuthMethod.DIGEST
                  .getMechanismName(), null, SaslUtil.SASL_DEFAULT_REALM,
                  HBaseSaslRpcServer.getSaslProps(), new SaslDigestCallbackHandler(
                      secretManager, this));
              break;
            default:
              UserGroupInformation current = UserGroupInformation.getCurrentUser();
              String fullName = current.getUserName();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Kerberos principal name is " + fullName);
              }
              final String names[] = SaslUtil.splitKerberosName(fullName);
              if (names.length != 3) {
                throw new AccessDeniedException(
                    "Kerberos principal name does NOT have the expected "
                        + "hostname part: " + fullName);
              }
              current.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws SaslException {
                  saslServer = Sasl.createSaslServer(AuthMethod.KERBEROS
                      .getMechanismName(), names[0], names[1],
                      HBaseSaslRpcServer.getSaslProps(), new SaslGssCallbackHandler());
                  return null;
                }
              });
            }
            if (saslServer == null)
              throw new AccessDeniedException(
                  "Unable to find SASL server implementation for "
                      + authMethod.getMechanismName());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Created SASL server with mechanism = " + authMethod.getMechanismName());
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Have read input token of size " + saslToken.limit()
                + " for processing by saslServer.evaluateResponse()");
          }
          replyToken = saslServer
              .evaluateResponse(saslToken.hasArray() ? saslToken.array() : saslToken.toBytes());
        } catch (IOException e) {
          IOException sendToClient = e;
          Throwable cause = e;
          while (cause != null) {
            if (cause instanceof InvalidToken) {
              sendToClient = (InvalidToken) cause;
              break;
            }
            cause = cause.getCause();
          }
          doRawSaslReply(SaslStatus.ERROR, null, sendToClient.getClass().getName(),
            sendToClient.getLocalizedMessage());
          metrics.authenticationFailure();
          String clientIP = this.toString();
          // attempting user could be null
          AUDITLOG.warn(AUTH_FAILED_FOR + clientIP + ":" + attemptingUser);
          throw e;
        }
        if (replyToken != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Will send token of size " + replyToken.length
                + " from saslServer.");
          }
          doRawSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null,
              null);
        }
        if (saslServer.isComplete()) {
          String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
          useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
          ugi = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Authenticated client: "
              + ugi + ". Negotiated QoP is "
              + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          metrics.authenticationSuccess();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + ugi);
          saslContextEstablished = true;
        }
      }
    }

    private void processUnwrappedData(byte[] inBuf) throws IOException,
    InterruptedException {
      ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        int count;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

          if (unwrappedDataLength == RpcClient.PING_CALL_ID) {
            if (LOG.isDebugEnabled())
              LOG.debug("Received ping message");
            unwrappedDataLengthBuffer.clear();
            continue; // ping message
          }
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          processOneRpc(new SingleByteBuff(unwrappedData));
          unwrappedData = null;
        }
      }
    }

    public void processOneRpc(ByteBuff buf) throws IOException,
        InterruptedException {
      if (connectionHeaderRead) {
        processRequest(buf);
      } else {
        processConnectionHeader(buf);
        this.connectionHeaderRead = true;
        if (!authorizeConnection()) {
          // Throw FatalConnectionException wrapping ACE so client does right thing and closes
          // down the connection instead of trying to read non-existent retun.
          throw new AccessDeniedException("Connection from " + this + " for service " +
            connectionHeader.getServiceName() + " is unauthorized for user: " + ugi);
        }
        this.user = userProvider.create(this.ugi);
      }
    }

    protected boolean authorizeConnection() throws IOException {
      try {
        // If auth method is DIGEST, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (ugi != null && ugi.getRealUser() != null
            && (authMethod != AuthMethod.DIGEST)) {
          ProxyUsers.authorize(ugi, this.getHostAddress(), conf);
        }
        authorize(ugi, connectionHeader, getHostInetAddress());
        metrics.authorizationSuccess();
      } catch (AuthorizationException ae) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connection authorization failed: " + ae.getMessage(), ae);
        }
        metrics.authorizationFailure();
        setupResponse(authFailedResponse, authFailedCall,
          new AccessDeniedException(ae), ae.getMessage());
        authFailedCall.sendResponseIfReady();
        return false;
      }
      return true;
    }

    // Reads the connection header following version
    protected void processConnectionHeader(ByteBuff buf) throws IOException {
      if (buf.hasArray()) {
        this.connectionHeader = ConnectionHeader.parseFrom(buf.array());
      } else {
        CodedInputStream cis = UnsafeByteOperations
            .unsafeWrap(new ByteBuffByteInput(buf, 0, buf.limit()), 0, buf.limit()).newCodedInput();
        cis.enableAliasing(true);
        this.connectionHeader = ConnectionHeader.parseFrom(cis);
      }
      String serviceName = connectionHeader.getServiceName();
      if (serviceName == null) throw new EmptyServiceNameException();
      this.service = getService(services, serviceName);
      if (this.service == null) throw new UnknownServiceException(serviceName);
      setupCellBlockCodecs(this.connectionHeader);
      RPCProtos.ConnectionHeaderResponse.Builder chrBuilder =
          RPCProtos.ConnectionHeaderResponse.newBuilder();
      setupCryptoCipher(this.connectionHeader, chrBuilder);
      responseConnectionHeader(chrBuilder);
      UserGroupInformation protocolUser = createUser(connectionHeader);
      if (!useSasl) {
        ugi = protocolUser;
        if (ugi != null) {
          ugi.setAuthenticationMethod(AuthMethod.SIMPLE.authenticationMethod);
        }
        // audit logging for SASL authenticated users happens in saslReadAndProcess()
        if (authenticatedWithFallback) {
          LOG.warn("Allowed fallback to SIMPLE auth for " + ugi
              + " connecting from " + getHostAddress());
        }
        AUDITLOG.info(AUTH_SUCCESSFUL_FOR + ugi);
      } else {
        // user is authenticated
        ugi.setAuthenticationMethod(authMethod.authenticationMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However,
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(ugi.getUserName()))) {
          if (authMethod == AuthMethod.DIGEST) {
            // Not allowed to doAs if token authentication is used
            throw new AccessDeniedException("Authenticated user (" + ugi
                + ") doesn't match what the client claims to be ("
                + protocolUser + ")");
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = ugi;
            ugi = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
            // Now the user is a proxy user, set Authentication method Proxy.
            ugi.setAuthenticationMethod(AuthenticationMethod.PROXY);
          }
        }
      }
      if (connectionHeader.hasVersionInfo()) {
        // see if this connection will support RetryImmediatelyException
        retryImmediatelySupported = VersionInfoUtil.hasMinimumVersion(getVersionInfo(), 1, 2);

        AUDITLOG.info("Connection from " + this.hostAddress + " port: " + this.remotePort
            + " with version info: "
            + TextFormat.shortDebugString(connectionHeader.getVersionInfo()));
      } else {
        AUDITLOG.info("Connection from " + this.hostAddress + " port: " + this.remotePort
            + " with unknown version info");
      }
    }

    private void responseConnectionHeader(RPCProtos.ConnectionHeaderResponse.Builder chrBuilder)
        throws FatalConnectionException {
      // Response the connection header if Crypto AES is enabled
      if (!chrBuilder.hasCryptoCipherMeta()) return;
      try {
        byte[] connectionHeaderResBytes = chrBuilder.build().toByteArray();
        // encrypt the Crypto AES cipher meta data with sasl server, and send to client
        byte[] unwrapped = new byte[connectionHeaderResBytes.length + 4];
        Bytes.putBytes(unwrapped, 0, Bytes.toBytes(connectionHeaderResBytes.length), 0, 4);
        Bytes.putBytes(unwrapped, 4, connectionHeaderResBytes, 0, connectionHeaderResBytes.length);

        doConnectionHeaderResponse(saslServer.wrap(unwrapped, 0, unwrapped.length));
      } catch (IOException ex) {
        throw new UnsupportedCryptoException(ex.getMessage(), ex);
      }
    }

    /**
     * Send the response for connection header
     */
    private void doConnectionHeaderResponse(byte[] wrappedCipherMetaData)
        throws IOException {
      ByteBufferOutputStream response = null;
      DataOutputStream out = null;
      try {
        response = new ByteBufferOutputStream(wrappedCipherMetaData.length + 4);
        out = new DataOutputStream(response);
        out.writeInt(wrappedCipherMetaData.length);
        out.write(wrappedCipherMetaData);

        setConnectionHeaderResponseCall.setConnectionHeaderResponse(response
            .getByteBuffer());
        setConnectionHeaderResponseCall.sendResponseIfReady();
      } finally {
        if (out != null) {
          out.close();
        }
        if (response != null) {
          response.close();
        }
      }
    }

    /**
     * @param buf
     *          Has the request header and the request param and optionally
     *          encoded data buffer all in this one array.
     * @throws IOException
     * @throws InterruptedException
     */
    protected void processRequest(ByteBuff buf) throws IOException,
        InterruptedException {
      long totalRequestSize = buf.limit();
      int offset = 0;
      // Here we read in the header. We avoid having pb
      // do its default 4k allocation for CodedInputStream. We force it to use
      // backing array.
      CodedInputStream cis;
      if (buf.hasArray()) {
        cis = UnsafeByteOperations.unsafeWrap(buf.array(), 0, buf.limit())
            .newCodedInput();
      } else {
        cis = UnsafeByteOperations.unsafeWrap(
            new ByteBuffByteInput(buf, 0, buf.limit()), 0, buf.limit())
            .newCodedInput();
      }
      cis.enableAliasing(true);
      int headerSize = cis.readRawVarint32();
      offset = cis.getTotalBytesRead();
      Message.Builder builder = RequestHeader.newBuilder();
      ProtobufUtil.mergeFrom(builder, cis, headerSize);
      RequestHeader header = (RequestHeader) builder.build();
      offset += headerSize;
      int id = header.getCallId();
      if (LOG.isTraceEnabled()) {
        LOG.trace("RequestHeader " + TextFormat.shortDebugString(header)
            + " totalRequestSize: " + totalRequestSize + " bytes");
      }
      // Enforcing the call queue size, this triggers a retry in the client
      // This is a bit late to be doing this check - we have already read in the
      // total request.
      if ((totalRequestSize + callQueueSizeInBytes.sum()) > maxQueueSizeInBytes) {
        final ServerCall callTooBig = createCall(id, this.service, null,
            null, null, null, this, totalRequestSize, null, null, 0,
            this.callCleanup);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, callTooBig, CALL_QUEUE_TOO_BIG_EXCEPTION,
            "Call queue is full on " + server.getServerName()
                + ", is hbase.ipc.server.max.callqueue.size too small?");
        callTooBig.sendResponseIfReady();
        return;
      }
      MethodDescriptor md = null;
      Message param = null;
      CellScanner cellScanner = null;
      try {
        if (header.hasRequestParam() && header.getRequestParam()) {
          md = this.service.getDescriptorForType().findMethodByName(
              header.getMethodName());
          if (md == null)
            throw new UnsupportedOperationException(header.getMethodName());
          builder = this.service.getRequestPrototype(md).newBuilderForType();
          cis.resetSizeCounter();
          int paramSize = cis.readRawVarint32();
          offset += cis.getTotalBytesRead();
          if (builder != null) {
            ProtobufUtil.mergeFrom(builder, cis, paramSize);
            param = builder.build();
          }
          offset += paramSize;
        } else {
          // currently header must have request param, so we directly throw
          // exception here
          String msg = "Invalid request header: "
              + TextFormat.shortDebugString(header)
              + ", should have param set in it";
          LOG.warn(msg);
          throw new DoNotRetryIOException(msg);
        }
        if (header.hasCellBlockMeta()) {
          buf.position(offset);
          ByteBuff dup = buf.duplicate();
          dup.limit(offset + header.getCellBlockMeta().getLength());
          cellScanner = cellBlockBuilder.createCellScannerReusingBuffers(
              this.codec, this.compressionCodec, dup);
        }
      } catch (Throwable t) {
        InetSocketAddress address = getListenerAddress();
        String msg = (address != null ? address : "(channel closed)")
            + " is unable to read call parameter from client "
            + getHostAddress();
        LOG.warn(msg, t);

        metrics.exception(t);

        // probably the hbase hadoop version does not match the running hadoop
        // version
        if (t instanceof LinkageError) {
          t = new DoNotRetryIOException(t);
        }
        // If the method is not present on the server, do not retry.
        if (t instanceof UnsupportedOperationException) {
          t = new DoNotRetryIOException(t);
        }

        final ServerCall readParamsFailedCall = createCall(id,
            this.service, null, null, null, null, this, totalRequestSize, null,
            null, 0, this.callCleanup);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        setupResponse(responseBuffer, readParamsFailedCall, t,
            msg + "; " + t.getMessage());
        readParamsFailedCall.sendResponseIfReady();
        return;
      }

      TraceInfo traceInfo = header.hasTraceInfo() ? new TraceInfo(header
          .getTraceInfo().getTraceId(), header.getTraceInfo().getParentId())
          : null;
      int timeout = 0;
      if (header.hasTimeout() && header.getTimeout() > 0) {
        timeout = Math.max(minClientRequestTimeout, header.getTimeout());
      }
      ServerCall call = createCall(id, this.service, md, header, param,
          cellScanner, this, totalRequestSize, traceInfo, this.addr, timeout,
          this.callCleanup);

      if (!scheduler.dispatch(new CallRunner(RpcServer.this, call))) {
        callQueueSizeInBytes.add(-1 * call.getSize());

        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, call, CALL_QUEUE_TOO_BIG_EXCEPTION,
            "Call queue is full on " + server.getServerName()
                + ", too many items queued ?");
        call.sendResponseIfReady();
      }
    }

    public abstract boolean isConnectionOpen();

    public abstract ServerCall createCall(int id, final BlockingService service,
        final MethodDescriptor md, RequestHeader header, Message param,
        CellScanner cellScanner, Connection connection, long size,
        TraceInfo tinfo, final InetAddress remoteAddress, int timeout,
        CallCleanup reqCleanup);
  }

  /**
   * Datastructure for passing a {@link BlockingService} and its associated class of
   * protobuf service interface.  For example, a server that fielded what is defined
   * in the client protobuf service would pass in an implementation of the client blocking service
   * and then its ClientService.BlockingInterface.class.  Used checking connection setup.
   */
  public static class BlockingServiceAndInterface {
    private final BlockingService service;
    private final Class<?> serviceInterface;
    public BlockingServiceAndInterface(final BlockingService service,
        final Class<?> serviceInterface) {
      this.service = service;
      this.serviceInterface = serviceInterface;
    }
    public Class<?> getServiceInterface() {
      return this.serviceInterface;
    }
    public BlockingService getBlockingService() {
      return this.service;
    }
  }

  /**
   * Constructs a server listening on the named port and address.
   * @param server hosting instance of {@link Server}. We will do authentications if an
   * instance else pass null for no authentication check.
   * @param name Used keying this rpc servers' metrics and for naming the Listener thread.
   * @param services A list of services.
   * @param bindAddress Where to listen
   * @param conf
   * @param scheduler
   */
  public RpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress bindAddress, Configuration conf,
      RpcScheduler scheduler)
      throws IOException {
    if (conf.getBoolean("hbase.ipc.server.reservoir.enabled", true)) {
      int poolBufSize = conf.getInt(ByteBufferPool.BUFFER_SIZE_KEY,
          ByteBufferPool.DEFAULT_BUFFER_SIZE);
      // The max number of buffers to be pooled in the ByteBufferPool. The default value been
      // selected based on the #handlers configured. When it is read request, 2 MB is the max size
      // at which we will send back one RPC request. Means max we need 2 MB for creating the
      // response cell block. (Well it might be much lesser than this because in 2 MB size calc, we
      // include the heap size overhead of each cells also.) Considering 2 MB, we will need
      // (2 * 1024 * 1024) / poolBufSize buffers to make the response cell block. Pool buffer size
      // is by default 64 KB.
      // In case of read request, at the end of the handler process, we will make the response
      // cellblock and add the Call to connection's response Q and a single Responder thread takes
      // connections and responses from that one by one and do the socket write. So there is chances
      // that by the time a handler originated response is actually done writing to socket and so
      // released the BBs it used, the handler might have processed one more read req. On an avg 2x
      // we consider and consider that also for the max buffers to pool
      int bufsForTwoMB = (2 * 1024 * 1024) / poolBufSize;
      int maxPoolSize = conf.getInt(ByteBufferPool.MAX_POOL_SIZE_KEY,
          conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
              HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT) * bufsForTwoMB * 2);
      this.reservoir = new ByteBufferPool(poolBufSize, maxPoolSize);
      this.minSizeForReservoirUse = getMinSizeForReservoirUse(this.reservoir);
    } else {
      reservoir = null;
      this.minSizeForReservoirUse = Integer.MAX_VALUE;// reservoir itself not in place.
    }
    this.server = server;
    this.services = services;
    this.bindAddress = bindAddress;
    this.conf = conf;
    // See declaration above for documentation on what this size is.
    this.maxQueueSizeInBytes =
      this.conf.getLong("hbase.ipc.server.max.callqueue.size", DEFAULT_MAX_CALLQUEUE_SIZE);

    this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME, DEFAULT_WARN_RESPONSE_TIME);
    this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE, DEFAULT_WARN_RESPONSE_SIZE);
    this.minClientRequestTimeout = conf.getInt(MIN_CLIENT_REQUEST_TIMEOUT,
        DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT);
    this.maxRequestSize = conf.getInt(MAX_REQUEST_SIZE, DEFAULT_MAX_REQUEST_SIZE);

    this.metrics = new MetricsHBaseServer(name, new MetricsHBaseServerWrapperImpl(this));
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.server.tcpnodelay", true);
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.server.tcpkeepalive", true);

    this.cellBlockBuilder = new CellBlockBuilder(conf);

    this.authorize = conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
    this.userProvider = UserProvider.instantiate(conf);
    this.isSecurityEnabled = userProvider.isHBaseSecurityEnabled();
    if (isSecurityEnabled) {
      HBaseSaslRpcServer.init(conf);
    }

    this.scheduler = scheduler;
  }

  @VisibleForTesting
  static int getMinSizeForReservoirUse(ByteBufferPool pool) {
    return pool.getBufferSize() / 6;
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    initReconfigurable(newConf);
    if (scheduler instanceof ConfigurationObserver) {
      ((ConfigurationObserver) scheduler).onConfigurationChange(newConf);
    }
  }

  protected void initReconfigurable(Configuration confToLoad) {
    this.allowFallbackToSimpleAuth = confToLoad.getBoolean(FALLBACK_TO_INSECURE_CLIENT_AUTH, false);
    if (isSecurityEnabled && allowFallbackToSimpleAuth) {
      LOG.warn("********* WARNING! *********");
      LOG.warn("This server is configured to allow connections from INSECURE clients");
      LOG.warn("(" + FALLBACK_TO_INSECURE_CLIENT_AUTH + " = true).");
      LOG.warn("While this option is enabled, client identities cannot be secured, and user");
      LOG.warn("impersonation is possible!");
      LOG.warn("For secure operation, please disable SIMPLE authentication as soon as possible,");
      LOG.warn("by setting " + FALLBACK_TO_INSECURE_CLIENT_AUTH + " = false in hbase-site.xml");
      LOG.warn("****************************");
    }
  }

  /**
   * Setup response for the RPC Call.
   * @param response buffer to serialize the response into
   * @param call {@link ServerCall} to which we are setting up the response
   * @param error error message, if the call failed
   * @throws IOException
   */
  protected void setupResponse(ByteArrayOutputStream response, ServerCall call, Throwable t,
      String error) throws IOException {
    if (response != null) response.reset();
    call.setResponse(null, null, t, error);
  }

  Configuration getConf() {
    return conf;
  }

  @Override
  public boolean isStarted() {
    return this.started;
  }

  @Override
  public synchronized void refreshAuthManager(PolicyProvider pp) {
    // Ignore warnings that this should be accessed in a static way instead of via an instance;
    // it'll break if you go via static route.
    this.authManager.refresh(this.conf, pp);
  }

  protected AuthenticationTokenSecretManager createSecretManager() {
    if (!isSecurityEnabled) return null;
    if (server == null) return null;
    Configuration conf = server.getConfiguration();
    long keyUpdateInterval =
        conf.getLong("hbase.auth.key.update.interval", 24*60*60*1000);
    long maxAge =
        conf.getLong("hbase.auth.token.max.lifetime", 7*24*60*60*1000);
    return new AuthenticationTokenSecretManager(conf, server.getZooKeeper(),
        server.getServerName().toString(), keyUpdateInterval, maxAge);
  }

  public SecretManager<? extends TokenIdentifier> getSecretManager() {
    return this.secretManager;
  }

  @SuppressWarnings("unchecked")
  public void setSecretManager(SecretManager<? extends TokenIdentifier> secretManager) {
    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;
  }

  /**
   * This is a server side method, which is invoked over RPC. On success
   * the return response has protobuf response payload. On failure, the
   * exception name and the stack trace are returned in the protobuf response.
   */
  @Override
  public Pair<Message, CellScanner> call(RpcCall call,
      MonitoredRPCHandler status) throws IOException {
    try {
      MethodDescriptor md = call.getMethod();
      Message param = call.getParam();
      status.setRPC(md.getName(), new Object[]{param},
        call.getReceiveTime());
      // TODO: Review after we add in encoded data blocks.
      status.setRPCPacket(param);
      status.resume("Servicing call");
      //get an instance of the method arg type
      HBaseRpcController controller = new HBaseRpcControllerImpl(call.getCellScanner());
      controller.setCallTimeout(call.getTimeout());
      Message result = call.getService().callBlockingMethod(md, controller, param);
      long receiveTime = call.getReceiveTime();
      long startTime = call.getStartTime();
      long endTime = System.currentTimeMillis();
      int processingTime = (int) (endTime - startTime);
      int qTime = (int) (startTime - receiveTime);
      int totalTime = (int) (endTime - receiveTime);
      if (LOG.isTraceEnabled()) {
        LOG.trace(CurCall.get().toString() +
            ", response " + TextFormat.shortDebugString(result) +
            " queueTime: " + qTime +
            " processingTime: " + processingTime +
            " totalTime: " + totalTime);
      }
      // Use the raw request call size for now.
      long requestSize = call.getSize();
      long responseSize = result.getSerializedSize();
      if (call.isClientCellBlockSupported()) {
        // Include the payload size in HBaseRpcController
        responseSize += call.getResponseCellSize();
      }

      metrics.dequeuedCall(qTime);
      metrics.processedCall(processingTime);
      metrics.totalCall(totalTime);
      metrics.receivedRequest(requestSize);
      metrics.sentResponse(responseSize);
      // log any RPC responses that are slower than the configured warn
      // response time or larger than configured warning size
      boolean tooSlow = (processingTime > warnResponseTime && warnResponseTime > -1);
      boolean tooLarge = (responseSize > warnResponseSize && warnResponseSize > -1);
      if (tooSlow || tooLarge) {
        // when tagging, we let TooLarge trump TooSmall to keep output simple
        // note that large responses will often also be slow.
        logResponse(param,
            md.getName(), md.getName() + "(" + param.getClass().getName() + ")",
            (tooLarge ? "TooLarge" : "TooSlow"),
            status.getClient(), startTime, processingTime, qTime,
            responseSize);
      }
      return new Pair<>(result, controller.cellScanner());
    } catch (Throwable e) {
      // The above callBlockingMethod will always return a SE.  Strip the SE wrapper before
      // putting it on the wire.  Its needed to adhere to the pb Service Interface but we don't
      // need to pass it over the wire.
      if (e instanceof ServiceException) {
        if (e.getCause() == null) {
          LOG.debug("Caught a ServiceException with null cause", e);
        } else {
          e = e.getCause();
        }
      }

      // increment the number of requests that were exceptions.
      metrics.exception(e);

      if (e instanceof LinkageError) throw new DoNotRetryIOException(e);
      if (e instanceof IOException) throw (IOException)e;
      LOG.error("Unexpected throwable object ", e);
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * Logs an RPC response to the LOG file, producing valid JSON objects for
   * client Operations.
   * @param param The parameters received in the call.
   * @param methodName The name of the method invoked
   * @param call The string representation of the call
   * @param tag  The tag that will be used to indicate this event in the log.
   * @param clientAddress   The address of the client who made this call.
   * @param startTime       The time that the call was initiated, in ms.
   * @param processingTime  The duration that the call took to run, in ms.
   * @param qTime           The duration that the call spent on the queue
   *                        prior to being initiated, in ms.
   * @param responseSize    The size in bytes of the response buffer.
   */
  void logResponse(Message param, String methodName, String call, String tag,
      String clientAddress, long startTime, int processingTime, int qTime,
      long responseSize) throws IOException {
    // base information that is reported regardless of type of call
    Map<String, Object> responseInfo = new HashMap<>();
    responseInfo.put("starttimems", startTime);
    responseInfo.put("processingtimems", processingTime);
    responseInfo.put("queuetimems", qTime);
    responseInfo.put("responsesize", responseSize);
    responseInfo.put("client", clientAddress);
    responseInfo.put("class", server == null? "": server.getClass().getSimpleName());
    responseInfo.put("method", methodName);
    responseInfo.put("call", call);
    responseInfo.put("param", ProtobufUtil.getShortTextFormat(param));
    if (param instanceof ClientProtos.ScanRequest && rsRpcServices != null) {
      ClientProtos.ScanRequest request = ((ClientProtos.ScanRequest) param);
      if (request.hasScannerId()) {
        long scannerId = request.getScannerId();
        String scanDetails = rsRpcServices.getScanDetailsWithId(scannerId);
        if (scanDetails != null) {
          responseInfo.put("scandetails", scanDetails);
        }
      }
    }
    LOG.warn("(response" + tag + "): " + MAPPER.writeValueAsString(responseInfo));
  }

  /**
   * Set the handler for calling out of RPC for error conditions.
   * @param handler the handler implementation
   */
  @Override
  public void setErrorHandler(HBaseRPCErrorHandler handler) {
    this.errorHandler = handler;
  }

  @Override
  public HBaseRPCErrorHandler getErrorHandler() {
    return this.errorHandler;
  }

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  @Override
  public MetricsHBaseServer getMetrics() {
    return metrics;
  }

  @Override
  public void addCallSize(final long diff) {
    this.callQueueSizeInBytes.add(diff);
  }

  /**
   * Authorize the incoming client connection.
   *
   * @param user client user
   * @param connection incoming connection
   * @param addr InetAddress of incoming connection
   * @throws org.apache.hadoop.security.authorize.AuthorizationException
   *         when the client isn't authorized to talk the protocol
   */
  public synchronized void authorize(UserGroupInformation user,
      ConnectionHeader connection, InetAddress addr)
      throws AuthorizationException {
    if (authorize) {
      Class<?> c = getServiceInterface(services, connection.getServiceName());
      this.authManager.authorize(user != null ? user : null, c, getConf(), addr);
    }
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  protected static final int NIO_BUFFER_LIMIT = 64 * 1024; //should not be more than 64KB.

  /**
   * This is a wrapper around {@link java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * ByteBuffer increases. There should not be any performance degredation.
   *
   * @param channel writable byte channel to write on
   * @param buffer buffer to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
   */
  protected int channelRead(ReadableByteChannel channel,
                                   ByteBuffer buffer) throws IOException {

    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      metrics.receivedBytes(count);
    }
    return count;
  }

  /**
   * Helper for {@link #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)}
   * and {@link #channelWrite(GatheringByteChannel, BufferChain)}. Only
   * one of readCh or writeCh should be non-null.
   *
   * @param readCh read channel
   * @param writeCh write channel
   * @param buf buffer to read or write into/out of
   * @return bytes written
   * @throws java.io.IOException e
   * @see #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)
   * @see #channelWrite(GatheringByteChannel, BufferChain)
   */
  private static int channelIO(ReadableByteChannel readCh,
                               WritableByteChannel writeCh,
                               ByteBuffer buf) throws IOException {

    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;

    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);

        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);
      }
    }

    int nBytes = initialRemaining - buf.remaining();
    return (nBytes > 0) ? nBytes : ret;
  }

  /**
   * This is extracted to a static method for better unit testing. We try to get buffer(s) from pool
   * as much as possible.
   *
   * @param pool The ByteBufferPool to use
   * @param minSizeForPoolUse Only for buffer size above this, we will try to use pool. Any buffer
   *           need of size below this, create on heap ByteBuffer.
   * @param reqLen Bytes count in request
   */
  @VisibleForTesting
  static Pair<ByteBuff, CallCleanup> allocateByteBuffToReadInto(ByteBufferPool pool,
      int minSizeForPoolUse, int reqLen) {
    ByteBuff resultBuf;
    List<ByteBuffer> bbs = new ArrayList<>((reqLen / pool.getBufferSize()) + 1);
    int remain = reqLen;
    ByteBuffer buf = null;
    while (remain >= minSizeForPoolUse && (buf = pool.getBuffer()) != null) {
      bbs.add(buf);
      remain -= pool.getBufferSize();
    }
    ByteBuffer[] bufsFromPool = null;
    if (bbs.size() > 0) {
      bufsFromPool = new ByteBuffer[bbs.size()];
      bbs.toArray(bufsFromPool);
    }
    if (remain > 0) {
      bbs.add(ByteBuffer.allocate(remain));
    }
    if (bbs.size() > 1) {
      ByteBuffer[] items = new ByteBuffer[bbs.size()];
      bbs.toArray(items);
      resultBuf = new MultiByteBuff(items);
    } else {
      // We are backed by single BB
      resultBuf = new SingleByteBuff(bbs.get(0));
    }
    resultBuf.limit(reqLen);
    if (bufsFromPool != null) {
      final ByteBuffer[] bufsFromPoolFinal = bufsFromPool;
      return new Pair<>(resultBuf, () -> {
        // Return back all the BBs to pool
        for (int i = 0; i < bufsFromPoolFinal.length; i++) {
          pool.putbackBuffer(bufsFromPoolFinal[i]);
        }
      });
    }
    return new Pair<>(resultBuf, null);
  }

  /**
   * Needed for features such as delayed calls.  We need to be able to store the current call
   * so that we can complete it later or ask questions of what is supported by the current ongoing
   * call.
   * @return An RpcCallContext backed by the currently ongoing call (gotten from a thread local)
   */
  public static RpcCall getCurrentCall() {
    return CurCall.get();
  }

  public static boolean isInRpcCallContext() {
    return CurCall.get() != null;
  }

  /**
   * Returns the user credentials associated with the current RPC request or
   * <code>null</code> if no credentials were provided.
   * @return A User
   */
  public static User getRequestUser() {
    RpcCallContext ctx = getCurrentCall();
    return ctx == null? null: ctx.getRequestUser();
  }

  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  abstract public int getNumOpenConnections();

  /**
   * Returns the username for any user associated with the current RPC
   * request or <code>null</code> if no user is set.
   */
  public static String getRequestUserName() {
    User user = getRequestUser();
    return user == null? null: user.getShortName();
  }

  /**
   * @return Address of remote client if a request is ongoing, else null
   */
  public static InetAddress getRemoteAddress() {
    RpcCallContext ctx = getCurrentCall();
    return ctx == null? null: ctx.getRemoteAddress();
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services Available service instances
   * @return Matching BlockingServiceAndInterface pair
   */
  protected static BlockingServiceAndInterface getServiceAndInterface(
      final List<BlockingServiceAndInterface> services, final String serviceName) {
    for (BlockingServiceAndInterface bs : services) {
      if (bs.getBlockingService().getDescriptorForType().getName().equals(serviceName)) {
        return bs;
      }
    }
    return null;
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services Available services and their service interfaces.
   * @return Service interface class for <code>serviceName</code>
   */
  protected static Class<?> getServiceInterface(
      final List<BlockingServiceAndInterface> services,
      final String serviceName) {
    BlockingServiceAndInterface bsasi =
        getServiceAndInterface(services, serviceName);
    return bsasi == null? null: bsasi.getServiceInterface();
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services Available services and their service interfaces.
   * @return BlockingService that goes with the passed <code>serviceName</code>
   */
  protected static BlockingService getService(
      final List<BlockingServiceAndInterface> services,
      final String serviceName) {
    BlockingServiceAndInterface bsasi =
        getServiceAndInterface(services, serviceName);
    return bsasi == null? null: bsasi.getBlockingService();
  }

  protected static MonitoredRPCHandler getStatus() {
    // It is ugly the way we park status up in RpcServer.  Let it be for now.  TODO.
    MonitoredRPCHandler status = RpcServer.MONITORED_RPC.get();
    if (status != null) {
      return status;
    }
    status = TaskMonitor.get().createRPCStatus(Thread.currentThread().getName());
    status.pause("Waiting for a call");
    RpcServer.MONITORED_RPC.set(status);
    return status;
  }

  /** Returns the remote side ip address when invoked inside an RPC
   *  Returns null incase of an error.
   *  @return InetAddress
   */
  public static InetAddress getRemoteIp() {
    RpcCall call = CurCall.get();
    if (call != null) {
      return call.getRemoteAddress();
    }
    return null;
  }

  @Override
  public RpcScheduler getScheduler() {
    return scheduler;
  }

  @Override
  public void setRsRpcServices(RSRpcServices rsRpcServices) {
    this.rsRpcServices = rsRpcServices;
  }

  protected static class ByteBuffByteInput extends ByteInput {

    private ByteBuff buf;
    private int offset;
    private int length;

    ByteBuffByteInput(ByteBuff buf, int offset, int length) {
      this.buf = buf;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public byte read(int offset) {
      return this.buf.get(getAbsoluteOffset(offset));
    }

    private int getAbsoluteOffset(int offset) {
      return this.offset + offset;
    }

    @Override
    public int read(int offset, byte[] out, int outOffset, int len) {
      this.buf.get(getAbsoluteOffset(offset), out, outOffset, len);
      return len;
    }

    @Override
    public int read(int offset, ByteBuffer out) {
      int len = out.remaining();
      this.buf.get(out, getAbsoluteOffset(offset), len);
      return len;
    }

    @Override
    public int size() {
      return this.length;
    }
  }
}

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

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

/**
 * Base class of an RPC connection.
 */
@InterfaceAudience.Private
public abstract class Connection {

  private static final Log LOG = LogFactory.getLog(Connection.class);

  protected final ConnectionId remoteId;

  protected final int pingInterval;

  protected final AuthMethod authMethod;

  protected final boolean useSasl;

  protected final Token<? extends TokenIdentifier> token;

  protected final String serverPrincipal; // server's krb5 principal name

  protected final int reloginMaxBackoff; // max pause before relogin on sasl failure

  protected final Codec codec;

  protected final CompressionCodec compressor;

  protected final ConnectionHeader header; // connection header

  // the last time we were picked up from connection pool.
  protected long lastTouched;

  private UserInformation getUserInfo(UserGroupInformation ugi) {
    if (ugi == null || authMethod == AuthMethod.DIGEST) {
      // Don't send user for token auth
      return null;
    }
    UserInformation.Builder userInfoPB = UserInformation.newBuilder();
    if (authMethod == AuthMethod.KERBEROS) {
      // Send effective user for Kerberos auth
      userInfoPB.setEffectiveUser(ugi.getUserName());
    } else if (authMethod == AuthMethod.SIMPLE) {
      // Send both effective user and real user for simple auth
      userInfoPB.setEffectiveUser(ugi.getUserName());
      if (ugi.getRealUser() != null) {
        userInfoPB.setRealUser(ugi.getRealUser().getUserName());
      }
    }
    return userInfoPB.build();
  }

  protected Connection(Configuration conf, ConnectionId remoteId, String clusterId,
      boolean isSecurityEnabled, int pingInterval, final Codec codec,
      final CompressionCodec compressor) throws IOException {
    if (remoteId.getAddress().isUnresolved()) {
      throw new UnknownHostException("unknown host: " + remoteId.getAddress().getHostName());
    }
    this.codec = codec;
    this.compressor = compressor;

    UserGroupInformation ticket = remoteId.getTicket().getUGI();
    SecurityInfo securityInfo = SecurityInfo.getInfo(remoteId.getServiceName());
    this.useSasl = isSecurityEnabled;
    Token<? extends TokenIdentifier> token = null;
    String serverPrincipal = null;
    if (useSasl && securityInfo != null) {
      AuthenticationProtos.TokenIdentifier.Kind tokenKind = securityInfo.getTokenKind();
      if (tokenKind != null) {
        TokenSelector<? extends TokenIdentifier> tokenSelector = RpcClientImpl.TOKEN_HANDLERS
            .get(tokenKind);
        if (tokenSelector != null) {
          token = tokenSelector.selectToken(new Text(clusterId), ticket.getTokens());
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("No token selector found for type " + tokenKind);
        }
      }
      String serverKey = securityInfo.getServerPrincipal();
      if (serverKey == null) {
        throw new IOException("Can't obtain server Kerberos config key from SecurityInfo");
      }
      serverPrincipal = SecurityUtil.getServerPrincipal(conf.get(serverKey),
        remoteId.address.getAddress().getCanonicalHostName().toLowerCase());
      if (LOG.isDebugEnabled()) {
        LOG.debug("RPC Server Kerberos principal name for service=" + remoteId.getServiceName()
            + " is " + serverPrincipal);
      }
    }
    this.token = token;
    this.serverPrincipal = serverPrincipal;
    if (!useSasl) {
      authMethod = AuthMethod.SIMPLE;
    } else if (token != null) {
      authMethod = AuthMethod.DIGEST;
    } else {
      authMethod = AuthMethod.KERBEROS;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Use " + authMethod + " authentication for service " + remoteId.serviceName
          + ", sasl=" + useSasl);
    }
    reloginMaxBackoff = conf.getInt("hbase.security.relogin.maxbackoff", 5000);
    this.remoteId = remoteId;
    if (remoteId.rpcTimeout > 0) {
      this.pingInterval = remoteId.rpcTimeout; // overwrite pingInterval
    } else {
      this.pingInterval = pingInterval;
    }

    ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
    builder.setServiceName(remoteId.getServiceName());
    UserInformation userInfoPB;
    if ((userInfoPB = getUserInfo(ticket)) != null) {
      builder.setUserInfo(userInfoPB);
    }
    if (this.codec != null) {
      builder.setCellBlockCodecClass(this.codec.getClass().getCanonicalName());
    }
    if (this.compressor != null) {
      builder.setCellBlockCompressorClass(this.compressor.getClass().getCanonicalName());
    }
    builder.setVersionInfo(ProtobufUtil.getVersionInfo());
    this.header = builder.build();
  }

  /**
   * Write the RPC header: <MAGIC WORD -- 'HBas'> <ONEBYTE_VERSION> <ONEBYTE_AUTH_TYPE>
   */
  protected byte[] getConnectionHeaderPreamble() {
    // Assemble the preamble up in a buffer first and then send it. Writing individual elements,
    // they are getting sent across piecemeal according to wireshark and then server is messing
    // up the reading on occasion (the passed in stream is not buffered yet).

    // Preamble is six bytes -- 'HBas' + VERSION + AUTH_CODE
    int rpcHeaderLen = HConstants.RPC_HEADER.array().length;
    byte[] preamble = new byte[rpcHeaderLen + 2];
    System.arraycopy(HConstants.RPC_HEADER.array(), 0, preamble, 0, rpcHeaderLen);
    preamble[rpcHeaderLen] = HConstants.RPC_CURRENT_VERSION;
    preamble[rpcHeaderLen + 1] = authMethod.code;
    return preamble;
  }

  protected UserGroupInformation getUGI() {
    UserGroupInformation ticket = remoteId.getTicket().getUGI();
    if (authMethod == AuthMethod.KERBEROS) {
      if (ticket != null && ticket.getRealUser() != null) {
        ticket = ticket.getRealUser();
      }
    }
    return ticket;
  }

  protected boolean shouldRelogin(Throwable e) throws IOException {
    if (e instanceof FallbackDisallowedException) {
      return false;
    }
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUser = currentUser.getRealUser();
    return authMethod == AuthMethod.KERBEROS && loginUser != null &&
    // Make sure user logged in using Kerberos either keytab or TGT
        loginUser.hasKerberosCredentials() &&
        // relogin only in case it is the login user (e.g. JT)
        // or superuser (like oozie).
        (loginUser.equals(currentUser) || loginUser.equals(realUser));
  }

  protected void relogin() throws IOException {
    if (UserGroupInformation.isLoginKerberosKeyBased()) {
      UserGroupInformation.getLoginUser().reloginFromKerberosKey();
    } else {
      UserGroupInformation.getLoginUser().reloginFromTicketCache();
    }
  }

  public ConnectionId remoteId() {
    return remoteId;
  }

  public long getLastTouched() {
    return lastTouched;
  }

  public void setLastTouched(long lastTouched) {
    this.lastTouched = lastTouched;
  }

  /**
   * Just close connection. Do not need to remove from connection pool.
   */
  public abstract void shutdown();

  public abstract void sendRequest(Call call) throws IOException;
}

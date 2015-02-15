/**
 * @(#)KrbTestBase.java, 2013-11-29. 
 * 
 * Copyright 2013 Youdao, Inc. All rights reserved.
 * YOUDAO PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.apache.hadoop.hbase.security;

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import org.apache.commons.io.FileUtils;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.api.LdapCoreSessionConnection;
import org.apache.directory.server.core.api.partition.Partition;
import org.apache.directory.server.core.factory.DefaultDirectoryServiceFactory;
import org.apache.directory.server.core.factory.LdifPartitionFactory;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.kerberos.KerberosConfig;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.protocol.shared.transport.UdpTransport;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;

/**
 * @author zhangduo
 */
public class KrbTestBase {

  protected File testDir = new File(getClass().getName());

  protected String domainDn = "dc=youdao,dc=com";

  protected String domainDc = "youdao";

  protected String userDn = "ou=users," + domainDn;

  protected String userOu = "users";

  protected String realm = "YOUDAO.COM";

  protected KdcServer kdc;

  protected int boundPort = 45678;

  protected String uid = "zhangduo";

  protected String password = "123456";

  protected String serviceProtocol = "odis";

  protected final String serviceAddress;

  protected String randPass = "hehehehehe";

  protected File keyTabFile = new File(testDir, "keytab");

  protected long minTicketLifeTime = TimeUnit.MINUTES.toMillis(4);

  protected long maxTicketLifeTime = TimeUnit.DAYS.toMillis(1);

  protected KrbTestBase() {
    try {
      serviceAddress = InetAddress.getLocalHost().getHostName().toLowerCase();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private void outputKrb5Conf() throws IOException {
    File krb5Conf = new File(testDir, "krb5.conf");
    PrintWriter pw = new PrintWriter(krb5Conf);
    pw.println("[libdefaults]");
    pw.println("\tdefault_realm = " + realm);
    pw.println("[realms]");
    pw.println(realm + " = {");
    pw.println("\tkdc = localhost:" + boundPort);
    pw.println("}");
    pw.close();
    System.err.println("Output krb5.conf to " + krb5Conf.getAbsolutePath());
    System.setProperty("java.security.krb5.conf", krb5Conf.getAbsolutePath());
  }

  private void createPrincipal(LdapCoreSessionConnection conn, String rdn, String sn, String cn,
      String uid, String userPassword, String principalName) throws LdapException {
    Entry entry = new DefaultEntry();
    entry.setDn(rdn + "," + userDn);
    entry.add("objectClass", "top", "person", "inetOrgPerson", "krb5principal", "krb5kdcentry");
    entry.add("cn", cn);
    entry.add("sn", sn);
    entry.add("uid", uid);
    entry.add("userPassword", userPassword);
    entry.add("krb5PrincipalName", principalName);
    entry.add("krb5KeyVersionNumber", "0");
    conn.add(entry);
    System.err.println("add principal " + entry);
  }

  private void addEntry(DirectoryService ds) throws LdapException, IOException {
    LdapCoreSessionConnection conn = new LdapCoreSessionConnection(ds);
    Entry entry = new DefaultEntry();
    entry.setDn(domainDn);
    entry.add("dc", domainDc);
    entry.add("objectClass", "top", "domain");
    conn.add(entry);
    entry = new DefaultEntry();
    entry.setDn(userDn);
    entry.add("ou", userOu);
    entry.add("objectClass", "top", "organizationalunit");
    conn.add(entry);
    // create principals
    createPrincipal(conn, "uid=" + uid, "Last", "First Last", uid, password, uid + "@" + realm);
    createPrincipal(conn, "uid=krbtgt", "KDC Service", "KDC Service", "krbtgt", randPass, "krbtgt/"
        + realm + "@" + realm);
    createPrincipal(conn, "uid=" + serviceProtocol, serviceProtocol + " Service", serviceProtocol
        + " Service", serviceProtocol, randPass, serviceProtocol + "/" + serviceAddress + "@"
        + realm);
    conn.close();
  }

  private void writeKeyTab() throws IOException {
    KerberosTime timeStamp = new KerberosTime();
    long principalType = 1L; // KRB5_NT_PRINCIPAL

    Keytab keytab = Keytab.getInstance();
    List<KeytabEntry> entries = new ArrayList<KeytabEntry>();
    String userPrincipal = uid + "@" + realm;
    for (Map.Entry<EncryptionType, EncryptionKey> keyEntry : KerberosKeyFactory.getKerberosKeys(
      userPrincipal, password).entrySet()) {
      EncryptionKey key = keyEntry.getValue();
      byte keyVersion = (byte) key.getKeyVersion();
      entries.add(new KeytabEntry(userPrincipal, principalType, timeStamp, keyVersion, key));
    }
    String servicePrincipal = serviceProtocol + "/" + serviceAddress + "@" + realm;
    for (Map.Entry<EncryptionType, EncryptionKey> keyEntry : KerberosKeyFactory.getKerberosKeys(
      servicePrincipal, randPass).entrySet()) {
      EncryptionKey key = keyEntry.getValue();
      byte keyVersion = (byte) key.getKeyVersion();
      entries.add(new KeytabEntry(servicePrincipal, principalType, timeStamp, keyVersion, key));
    }
    keytab.setEntries(entries);
    keytab.write(keyTabFile);
  }

  protected void startKdc() throws Exception {
    FileUtils.deleteDirectory(testDir);
    testDir.mkdirs();
    outputKrb5Conf();
    System.setProperty("workingDirectory",
      new File(testDir.getAbsolutePath(), "ldap").getAbsolutePath());
    DefaultDirectoryServiceFactory factory = new DefaultDirectoryServiceFactory();
    factory.init(getClass().getSimpleName());
    DirectoryService ds = factory.getDirectoryService();
    ds.addFirst(new KeyDerivationInterceptor());
    LdifPartitionFactory pf = new LdifPartitionFactory();
    Partition partition =
        pf.createPartition(ds.getSchemaManager(), domainDc, domainDn, 100, new File(ds
            .getInstanceLayout().getPartitionsDirectory(), domainDc));
    ds.addPartition(partition);
    addEntry(ds);
    KerberosConfig conf = new KerberosConfig();
    conf.setPrimaryRealm(realm);
    conf.setSearchBaseDn(domainDn);
    conf.setMinimumTicketLifetime(minTicketLifeTime);
    conf.setMaximumTicketLifetime(maxTicketLifeTime);
    kdc = new KdcServer(conf);
    kdc.setDirectoryService(ds);
    kdc.setTransports(new UdpTransport(boundPort));
    kdc.start();
//    boundPort =
//        ((InetSocketAddress) kdc.getTransports()[0].getAcceptor().getLocalAddress()).getPort();
    System.err.println("UDP kdc server start at port " + boundPort);
    writeKeyTab();
  }

  protected void stopKdc() throws Exception {
    if (kdc != null) {
      DirectoryService ds = kdc.getDirectoryService();
      kdc.stop();
      kdc = null;
      ds.shutdown();
    }
  }

  protected void clearFS() throws IOException {
    FileUtils.deleteDirectory(testDir);
  }

  private Charset CHARSET = Charset.forName("ISO-8859-1");

  private void writeCountedOctetString(DataOutput out, String str) throws IOException {
    byte[] encoded = str.getBytes(CHARSET);
    out.writeInt(encoded.length);
    out.write(encoded);
  }

  private void writePrincipal(DataOutput out, KerberosPrincipal principal) throws IOException {
    out.writeInt(principal.getNameType());
    String name = principal.getName();
    String components = name.substring(0, name.indexOf('@'));
    int indexOfSlash = components.indexOf('/');
    out.writeInt(indexOfSlash < 0 ? 1 : 2);
    writeCountedOctetString(out, principal.getRealm());
    if (indexOfSlash < 0) {
      writeCountedOctetString(out, components);
    } else {
      String primary = components.substring(0, indexOfSlash);
      writeCountedOctetString(out, primary);
      String instance = components.substring(indexOfSlash + 1);
      writeCountedOctetString(out, instance);
    }
  }

  private void writeKey(DataOutput out, int keyType, SecretKey key) throws IOException {
    out.writeShort(keyType);
    out.writeShort(0);
    byte[] encoded = key.getEncoded();
    out.writeShort(encoded.length);
    out.write(encoded);
  }

  private void writeFlags(DataOutput out, boolean[] flags) throws IOException {
    int bits = 0;
    for (int i = 0; i < flags.length; i++) {
      bits |= (flags[i] ? 1 : 0) << i;
    }
    out.writeInt(bits);
  }

  private void writeTicket(DataOutput out, KerberosTicket ticket) throws IOException {
    byte[] encoded = ticket.getEncoded();
    out.writeInt(encoded.length);
    out.write(encoded);
  }

  private void writeTime(DataOutput out, Date time) throws IOException {
    if (time == null) {
      out.writeInt(0);
    } else {
      out.writeInt((int) (time.getTime() / 1000));
    }
  }

  protected void writeTicketCache(File ticketCacheFile, KerberosTicket ticket) throws IOException {
    DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(ticketCacheFile)));
    out.writeShort(0x0504);
    out.writeShort(0); // ignore delta time header
    writePrincipal(out, ticket.getClient()); // client is the primary
    writePrincipal(out, ticket.getClient());
    writePrincipal(out, ticket.getServer());
    writeKey(out, ticket.getSessionKeyType(), ticket.getSessionKey());
    writeTime(out, ticket.getAuthTime());
    writeTime(out, ticket.getStartTime());
    writeTime(out, ticket.getEndTime());
    writeTime(out, ticket.getRenewTill());
    out.writeByte(0); // we only store tgt
    writeFlags(out, ticket.getFlags());
    out.writeInt(0); // ignore addresses...
    out.writeInt(0); // ignore authdatas...
    writeTicket(out, ticket);
    out.writeInt(0); // no second_ticket
    out.close();
  }
}

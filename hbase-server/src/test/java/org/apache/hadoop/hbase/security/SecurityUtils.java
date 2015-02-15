/**
 * @(#)SecurityUtils.java, 2013-9-30. 
 * 
 * Copyright 2013 Youdao, Inc. All rights reserved.
 * YOUDAO PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package org.apache.hadoop.hbase.security;

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sun.security.auth.module.Krb5LoginModule;

public class SecurityUtils {

  private static final Log LOG = LogFactory.getLog(SecurityUtils.class);

  /**
   * Return one element in the set.
   * @param set
   * @return
   */
  public static <T> T any(Set<T> set) {
    for (T t : set) {
      return t;
    }
    return null;
  }

  /**
   * Return one principal in the set.
   * @param subject
   * @param c
   * @return
   */
  public static <T extends Principal> T getPrincipal(Subject subject, Class<T> c) {
    return any(subject.getPrincipals(c));
  }

  /**
   * Return one private credential in the set.
   * @param subject
   * @param c
   * @return
   */
  public static <T> T getPrivateCredential(Subject subject, Class<T> c) {
    return any(subject.getPrivateCredentials(c));
  }

  /**
   * Get kerberos ticket granting ticket.
   * @param subject
   * @return
   */
  public static KerberosTicket getTGT(Subject subject) {
    for (KerberosTicket ticket : subject.getPrivateCredentials(KerberosTicket.class)) {
      if (ticket.getServer().getName().startsWith(KRB_TGT_SERVER_PRINCIPAL_NAME)) {
        return ticket;
      }
    }
    return null;
  }

  private static final String OS_LOGIN_MODULE_NAME;

  private static final Class<? extends Principal> OS_PRINCIPAL_CLASS;

  static {
    String osPrincipalClassName;
    OS_LOGIN_MODULE_NAME = "com.sun.security.auth.module.UnixLoginModule";
    osPrincipalClassName = "com.sun.security.auth.UnixPrincipal";
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    try {
      OS_PRINCIPAL_CLASS = cl.loadClass(osPrincipalClassName).asSubclass(Principal.class);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static final String OS_LOGIN_CTX_NAME = "ODIS-OS";

  private static final Map<String, String> OS_LOGIN_OPTIONS;

  private static final AppConfigurationEntry[] OS_LOGIN_CONF;

  static {
    OS_LOGIN_OPTIONS = Collections.emptyMap();
    OS_LOGIN_CONF =
        new AppConfigurationEntry[] { new AppConfigurationEntry(OS_LOGIN_MODULE_NAME,
            LoginModuleControlFlag.REQUIRED, OS_LOGIN_OPTIONS) };

  }

  /**
   * Login using OS login module.
   * @return
   * @throws LoginException
   */
  public static LoginContext loginFromOS() throws LoginException {
    LoginContext ctx = new LoginContext(OS_LOGIN_CTX_NAME, null, null, new Configuration() {

      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return OS_LOGIN_CONF;
      }

    });
    ctx.login();
    return ctx;
  }

  private static <T extends Principal> T getCanonicalUser(Subject subject, Class<T> cls) {
    return any(subject.getPrincipals(cls));
  }

  /**
   * Get username from OS login user.
   * <p>
   * This username will not be changed by adding -Duser.name=xxx when start program.
   * @return
   * @throws LoginException
   */
  public static String getOSLoginUsername() throws LoginException {
    LoginContext ctx = loginFromOS();
    Principal principal = getCanonicalUser(ctx.getSubject(), OS_PRINCIPAL_CLASS);
    String username = principal.getName();
    safeLogout(ctx);
    return username;
  }

  public static final String KRB_TGT_SERVER_PRINCIPAL_NAME = "krbtgt";

  private static final boolean KRB_VERBOSE = "true".equalsIgnoreCase(System
      .getProperty("sun.security.krb5.debug"));

  private static final String KRB_LOGIN_MODULE_OPT_DO_NOT_PROMPT = "doNotPrompt";

  private static final String KRB_LOGIN_MODULE_OPT_USE_TICKET_CACHE = "useTicketCache";

  private static final String KRB_LOGIN_MODULE_OPT_TICKET_CACHE = "ticketCache";

  private static final String KRB_LOGIN_MODULE_OPT_USE_KEY_TAB = "useKeyTab";

  private static final String KRB_LOGIN_MODULE_OPT_KEY_TAB = "keyTab";

  private static final String KRB_LOGIN_MODULE_OPT_PRINCIPAL = "principal";

  private static final String KRB_LOGIN_MODULE_OPT_STORE_KEY = "storeKey";

  private static final String KRB_LOGIN_MODULE_OPT_RENEW_TGT = "renewTGT";

  private static final String KRB_LOGIN_MODULE_OPT_DEBUG = "debug";

  private static final String KRB_TICKET_CACHE_LOGIN_CTX_NAME = "ODIS-Kerberos-TicketCache";

  private static final Map<String, String> KRB_TICKET_CACHE_LOGIN_BASIC_OPTIONS;

  static {
    Map<String, String> options = new HashMap<String, String>();
    options.put(KRB_LOGIN_MODULE_OPT_DO_NOT_PROMPT, Boolean.TRUE.toString());
    options.put(KRB_LOGIN_MODULE_OPT_USE_TICKET_CACHE, Boolean.TRUE.toString());
    options.put(KRB_LOGIN_MODULE_OPT_RENEW_TGT, Boolean.FALSE.toString());
    options.put(KRB_LOGIN_MODULE_OPT_DEBUG, Boolean.toString(KRB_VERBOSE));
    KRB_TICKET_CACHE_LOGIN_BASIC_OPTIONS = Collections.unmodifiableMap(options);
  }

  /**
   * Login from kerberos ticket cache.
   * @param ticketCache ticketCache file path
   * @return
   * @throws LoginException
   */
  public static LoginContext loginFromKrbTicketCache(final String ticketCache)
      throws LoginException {
    LoginContext ctx =
        new LoginContext(KRB_TICKET_CACHE_LOGIN_CTX_NAME, null, null, new Configuration() {

          @Override
          public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options =
                new HashMap<String, String>(KRB_TICKET_CACHE_LOGIN_BASIC_OPTIONS);
            options.put(KRB_LOGIN_MODULE_OPT_TICKET_CACHE, ticketCache);
            return new AppConfigurationEntry[] { new AppConfigurationEntry(
                Krb5LoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, options) };

          }
        });
    ctx.login();
    return ctx;
  }

  private static final String KRB_KEY_TAB_LOGIN_CTX_NAME = "ODIS-Kerberos-KeyTab";

  private static final Map<String, String> KRB_KEY_TAB_LOGIN_BASIC_OPTIONS;

  static {
    Map<String, String> options = new HashMap<String, String>();
    options.put(KRB_LOGIN_MODULE_OPT_DO_NOT_PROMPT, Boolean.TRUE.toString());
    options.put(KRB_LOGIN_MODULE_OPT_USE_KEY_TAB, Boolean.TRUE.toString());
    options.put(KRB_LOGIN_MODULE_OPT_STORE_KEY, Boolean.TRUE.toString());
    options.put(KRB_LOGIN_MODULE_OPT_RENEW_TGT, Boolean.FALSE.toString());
    options.put(KRB_LOGIN_MODULE_OPT_DEBUG, Boolean.toString(KRB_VERBOSE));
    KRB_KEY_TAB_LOGIN_BASIC_OPTIONS = Collections.unmodifiableMap(options);
  }

  /**
   * Login from kerberos keyTab.
   * @param keyTab keyTab file path
   * @param principal principal name that want to login
   * @return
   * @throws LoginException
   */
  public static LoginContext loginFromKrbKeyTab(final String keyTab, final String principal)
      throws LoginException {
    LoginContext ctx =
        new LoginContext(KRB_KEY_TAB_LOGIN_CTX_NAME, null, null, new Configuration() {

          @Override
          public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options =
                new HashMap<String, String>(KRB_KEY_TAB_LOGIN_BASIC_OPTIONS);
            options.put(KRB_LOGIN_MODULE_OPT_KEY_TAB, keyTab);
            options.put(KRB_LOGIN_MODULE_OPT_PRINCIPAL, principal);
            return new AppConfigurationEntry[] { new AppConfigurationEntry(
                Krb5LoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, options) };

          }
        });
    ctx.login();
    return ctx;
  }

  /**
   * Log out without throwing any exception.
   * @param ctx
   */
  public static void safeLogout(LoginContext ctx) {
    if (ctx != null) {
      try {
        ctx.logout();
      } catch (Throwable t) {
        LOG.warn("logout " + ctx + " failed", t);
      }
    }
  }
}

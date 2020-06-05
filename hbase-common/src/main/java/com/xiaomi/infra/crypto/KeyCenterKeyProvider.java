/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.xiaomi.infra.crypto;

import java.io.IOException;
import java.security.Key;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.xiaomi.keycenter.agent.client.DataProtectionProvider;
import com.xiaomi.keycenter.common.iface.DataProtectionException;

import sun.misc.BASE64Decoder;

/**
 * Only used for Xiaomi. It use keycenter to encrypt or decrypt a key.
 */
@InterfaceAudience.Private
public class KeyCenterKeyProvider {

  private static Logger LOG = LoggerFactory.getLogger(KeyCenterKeyProvider.class);

  public static final String KEYCENTER_PROVIDER_ID = "hbase.crypto.keycenter.provider.id";
  public static final String CRYPTO_KEYCENTER_KEY = "hbase.crypto.keycenter.key";
  public static final String CRYPTO_KEYCENTER_OLD_KEY = "hbase.crypto.keycenter.oldkey";

  // Default value is false, only use for test as the test can't access keycenter
  @VisibleForTesting
  public static final String SKIP_ACCESS_KEYCENTER = "hbase.crypto.keycenter.skip.access";

  /*
   * The default encryption only support AES now.
   */
  private final static String DEFAULT_CIPHER_NAME = "AES";

  private static final Map<String, String> keyToWrapped = new HashMap<>();

  private static final Map<String, String> wrappedToKey = new HashMap<>();

  private static volatile DataProtectionProvider provider;

  private static final Object providerLock = new Object();

  private static final BASE64Decoder base64Decoder = new BASE64Decoder();

  public static byte[] wrapKey(byte[] keyBytes) {
    String key = Bytes.toStringBinary(keyBytes);
    if (!keyToWrapped.containsKey(key)) {
      throw new RuntimeException("Can't find wrapped key in cache, this should not happen!");
    }
    return Bytes.toBytesBinary(keyToWrapped.get(key));
  }

  public static Key unwrapKey(String wrappedKey) {
    if (!wrappedToKey.containsKey(wrappedKey)) {
      throw new RuntimeException("Can't find key in cache for wrapped key " + wrappedKey);
    }
    return new SecretKeySpec(Bytes.toBytesBinary(wrappedToKey.get(wrappedKey)),
        DEFAULT_CIPHER_NAME);
  }

  public static Key unwrapKey(byte[] wrappedkeyBytes) {
    return unwrapKey(Bytes.toStringBinary(wrappedkeyBytes));
  }

  /**
   * Only call this when RS started to load key cache from keycenter. No need to consider concurrent problem.
   *
   * @param wrappedKey
   * @return
   * @throws DataProtectionException
   */
  private static String decryptFromKeyCenter(String wrappedKey)
      throws DataProtectionException, IOException {
    return Bytes
        .toStringBinary(provider.decrypt(base64Decoder.decodeBuffer(wrappedKey), null, false));
  }

  public static void initKeyProvider(Configuration conf) throws IOException {
    if (conf.getBoolean(SKIP_ACCESS_KEYCENTER, false)) {
      return;
    }
    String sid = conf.get(KEYCENTER_PROVIDER_ID);
    if (sid == null || sid.isEmpty()) {
      throw new IOException("Need config " + KEYCENTER_PROVIDER_ID + " when use keycenter");
    }
    if (provider == null) {
      synchronized (providerLock) {
        if (provider == null) {
          provider = DataProtectionProvider.getProvider(conf.get(KEYCENTER_PROVIDER_ID));
        }
      }
    }
  }

  public static void loadCacheFromKeyCenter(Configuration conf)
      throws DataProtectionException, IOException {
    if (conf.getBoolean(SKIP_ACCESS_KEYCENTER, false)) {
      return;
    }
    initKeyProvider(conf);
    String wrappedKey = conf.get(CRYPTO_KEYCENTER_KEY);
    LOG.info("Try load cache from keycenter for wrapped key " + wrappedKey);
    addToCache(decryptFromKeyCenter(wrappedKey), wrappedKey);
    // The old key is needed when rorate a new key.
    // Because the old storefiles still use the old key.
    wrappedKey = conf.get(CRYPTO_KEYCENTER_OLD_KEY);
    if (wrappedKey != null) {
      LOG.info("Try load cache from keycenter for wrapped key " + wrappedKey);
      addToCache(decryptFromKeyCenter(wrappedKey), wrappedKey);
    }
  }

  @VisibleForTesting
  public static void addToCache(String key, String wrappedKey) {
    keyToWrapped.put(key, wrappedKey);
    wrappedToKey.put(wrappedKey, key);
  }
}

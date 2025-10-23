/*
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
package org.apache.hadoop.hbase.io.crypto.tls;

import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/test/java/org/apache/zookeeper/common/KeyStoreFileTypeTest.java">Base
 *      revision</a>
 */
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestKeyStoreFileType {

  @Test
  public void testGetPropertyValue() {
    Assertions.assertEquals("PEM", KeyStoreFileType.PEM.getPropertyValue());
    Assertions.assertEquals("JKS", KeyStoreFileType.JKS.getPropertyValue());
    Assertions.assertEquals("PKCS12", KeyStoreFileType.PKCS12.getPropertyValue());
    Assertions.assertEquals("BCFKS", KeyStoreFileType.BCFKS.getPropertyValue());
  }

  @Test
  public void testFromPropertyValue() {
    Assertions.assertEquals(KeyStoreFileType.PEM, KeyStoreFileType.fromPropertyValue("PEM"));
    Assertions.assertEquals(KeyStoreFileType.JKS, KeyStoreFileType.fromPropertyValue("JKS"));
    Assertions.assertEquals(KeyStoreFileType.PKCS12, KeyStoreFileType.fromPropertyValue("PKCS12"));
    Assertions.assertEquals(KeyStoreFileType.BCFKS, KeyStoreFileType.fromPropertyValue("BCFKS"));
    Assertions.assertNull(KeyStoreFileType.fromPropertyValue(""));
    Assertions.assertNull(KeyStoreFileType.fromPropertyValue(null));
  }

  @Test
  public void testFromPropertyValueIgnoresCase() {
    Assertions.assertEquals(KeyStoreFileType.PEM, KeyStoreFileType.fromPropertyValue("pem"));
    Assertions.assertEquals(KeyStoreFileType.JKS, KeyStoreFileType.fromPropertyValue("jks"));
    Assertions.assertEquals(KeyStoreFileType.PKCS12, KeyStoreFileType.fromPropertyValue("pkcs12"));
    Assertions.assertEquals(KeyStoreFileType.BCFKS, KeyStoreFileType.fromPropertyValue("bcfks"));
    Assertions.assertNull(KeyStoreFileType.fromPropertyValue(""));
    Assertions.assertNull(KeyStoreFileType.fromPropertyValue(null));
  }

  @Test
  public void testFromPropertyValueThrowsOnBadPropertyValue() {
    Assertions.assertThrows(IllegalArgumentException.class,
      () -> KeyStoreFileType.fromPropertyValue("foobar"));
  }

  @Test
  public void testFromFilename() {
    Assertions.assertEquals(KeyStoreFileType.JKS, KeyStoreFileType.fromFilename("mykey.jks"));
    Assertions.assertEquals(KeyStoreFileType.JKS,
      KeyStoreFileType.fromFilename("/path/to/key/dir/mykey.jks"));
    Assertions.assertEquals(KeyStoreFileType.PEM, KeyStoreFileType.fromFilename("mykey.pem"));
    Assertions.assertEquals(KeyStoreFileType.PEM,
      KeyStoreFileType.fromFilename("/path/to/key/dir/mykey.pem"));
    Assertions.assertEquals(KeyStoreFileType.PKCS12, KeyStoreFileType.fromFilename("mykey.p12"));
    Assertions.assertEquals(KeyStoreFileType.PKCS12,
      KeyStoreFileType.fromFilename("/path/to/key/dir/mykey.p12"));
    Assertions.assertEquals(KeyStoreFileType.BCFKS, KeyStoreFileType.fromFilename("mykey.bcfks"));
    Assertions.assertEquals(KeyStoreFileType.BCFKS,
      KeyStoreFileType.fromFilename("/path/to/key/dir/mykey.bcfks"));
  }

  @Test
  public void testFromFilenameThrowsOnBadFileExtension() {
    Assertions.assertThrows(IllegalArgumentException.class,
      () -> KeyStoreFileType.fromFilename("prod.key"));
  }

  @Test
  public void testFromPropertyValueOrFileName() {
    // Property value takes precedence if provided
    Assertions.assertEquals(KeyStoreFileType.JKS,
      KeyStoreFileType.fromPropertyValueOrFileName("JKS", "prod.key"));
    Assertions.assertEquals(KeyStoreFileType.PEM,
      KeyStoreFileType.fromPropertyValueOrFileName("PEM", "prod.key"));
    Assertions.assertEquals(KeyStoreFileType.PKCS12,
      KeyStoreFileType.fromPropertyValueOrFileName("PKCS12", "prod.key"));
    Assertions.assertEquals(KeyStoreFileType.BCFKS,
      KeyStoreFileType.fromPropertyValueOrFileName("BCFKS", "prod.key"));
    // Falls back to filename detection if no property value
    Assertions.assertEquals(KeyStoreFileType.JKS,
      KeyStoreFileType.fromPropertyValueOrFileName("", "prod.jks"));
  }

  @Test
  public void testFromPropertyValueOrFileNameThrowsOnBadPropertyValue() {
    Assertions.assertThrows(IllegalArgumentException.class,
      () -> KeyStoreFileType.fromPropertyValueOrFileName("foobar", "prod.jks"));
  }

  @Test
  public void testFromPropertyValueOrFileNameThrowsOnBadFileExtension() {
    Assertions.assertThrows(IllegalArgumentException.class,
      () -> KeyStoreFileType.fromPropertyValueOrFileName("", "prod.key"));
  }
}

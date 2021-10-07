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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.apache.hadoop.hbase.ClassFinder;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreFileTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileTracker.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestStoreFileTracker.class);

  /**
   * Assert that all StoreFileTracker implementation classes have the static persistConfiguration
   * method.
   */
  @Test
  public void testHasPersistConfiguration()
    throws ClassNotFoundException, IOException, LinkageError {
    ClassFinder cf = new ClassFinder(null, null, StoreFileTracker.class::isAssignableFrom);
    StringWriter sw = new StringWriter();
    try (PrintWriter pw = new PrintWriter(sw)) {
      for (Class<?> c : cf.findClasses(false)) {
        LOG.info("Checking {}", c.getName());
        if (c.isInterface() || Modifier.isAbstract(c.getModifiers())) {
          continue;
        }
        Method method;
        try {
          method = c.getMethod(StoreFileTrackerFactory.PERSIST_CONF_METHOD_NAME,
            StoreFileTrackerFactory.PERSIST_CONF_METHOD_PARAMS);
        } catch (NoSuchMethodException e) {
          pw.println("Error checking " + c.getName() + ": " + e.toString());
          continue;
        }
        if (!Modifier.isStatic(method.getModifiers())) {
          pw.println(
            "Error checking " + c.getName() + ": method " + method.getName() + " is not static");
        }
      }
    }
    String error = sw.toString();
    if (!error.isEmpty()) {
      LOG.info(error);
      fail("Checking fails, please see the above error message");
    }
  }
}

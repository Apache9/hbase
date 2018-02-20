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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunListener.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A RunListener to confirm that we have a {@link CategoryBasedTimeout} class rule for every test.
 */
@InterfaceAudience.Private
@ThreadSafe
public class HBaseClassTestRuleChecker extends RunListener {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseClassTestRuleChecker.class);

  private volatile Class<?> testClass;

  @Override
  public void testStarted(Description description) throws Exception {
    Category[] categories = description.getTestClass().getAnnotationsByType(Category.class);
    for (Class<?> c : categories[0].value()) {
      if (c == IntegrationTests.class) {
        return;
      }
    }
    for (Field field : description.getTestClass().getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == HBaseClassTestRule.class &&
        field.isAnnotationPresent(ClassRule.class)) {
        HBaseClassTestRule timeout = (HBaseClassTestRule) field.get(null);
        assertEquals(
          "The HBaseClassTestRule ClassRule in " + description.getTestClass().getName() +
            " is for " + timeout.getClazz().getName(),
          description.getTestClass(), timeout.getClazz());
        testClass = description.getTestClass();
        return;
      }
    }
    fail("No HBaseClassTestRule ClassRule for " + description.getTestClass().getName());
  }

  @Override
  public void testRunFinished(Result result) throws Exception {
    Class<?> testClass = this.testClass;
    if (testClass == null) {
      LOG.warn("No test class specified");
      return;
    }
    long seconds = TimeUnit.MILLISECONDS.toSeconds(result.getRunTime());
    Category[] categories = testClass.getAnnotationsByType(Category.class);
    for (Class<?> c : categories[0].value()) {
      if (c == SmallTests.class) {
        if (seconds > 15) {
          LOG.warn(testClass.getName() + " which has " + result.getRunCount() +
            " tests is declared as small but runs for " +
            String.format("%.03f", result.getRunTime() / 1000.0) + " seconds");
        }
      } else if (c == MediumTests.class) {
        if (seconds <= 15 || seconds > 90) {
          LOG.warn(testClass.getName() + " which has " + result.getRunCount() +
            " tests is declared as medium but runs for " +
            String.format("%.03f", result.getRunTime() / 1000.0) + " seconds");
        }
      } else if (c == LargeTests.class) {
        if (seconds <= 90 || seconds > 540) {
          LOG.warn(testClass.getName() + " which has " + result.getRunCount() +
            " tests is declared as large but runs for " +
            String.format("%.03f", result.getRunTime() / 1000.0) + " seconds");
        }
      }
    }
    this.testClass = null;
  }
}

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
package org.apache.hadoop.hbase.trace;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

@Category(SmallTests.class)
public class TestOpenTelemetryInitializer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOpenTelemetryInitializer.class);

  private Appender appender;

  @Before
  public void setUp() {
    appender = mock(Appender.class);
    LogManager.getLogger(HBaseLoggingSpanExporter.LOGGER_NAME).addAppender(appender);
  }

  @After
  public void tearDown() {
    LogManager.getLogger(HBaseLoggingSpanExporter.LOGGER_NAME).removeAppender(appender);
  }

  @Test
  public void test() throws InterruptedException {
    System.setProperty("otel.bsp.schedule.delay.millis", "100");
    TraceUtil.initialize(HBaseConfiguration.create());
    TraceUtil.getGlobalTracer().spanBuilder("testSpan").startSpan().end();
    Thread.sleep(1000);
    ArgumentCaptor<LoggingEvent> captor = ArgumentCaptor.forClass(LoggingEvent.class);
    verify(appender, atLeastOnce()).doAppend(captor.capture());
    for (LoggingEvent event : captor.getAllValues()) {
      if (event.getLevel() == Level.INFO && event.getRenderedMessage().contains("name=testSpan")) {
        return;
      }
    }
    fail("Should output the testSpan trace log");
  }
}

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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Initialize the OpenTelemetry trace system.
 */
@InterfaceAudience.Private
public class OpenTelemetryInitializer implements HBaseTraceInitializer {

  @Override
  public void init(Configuration conf) {
    if (!isTraceEnabled(conf)) {
      return;
    }
    SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder().build();
    sdkTracerProvider.addSpanProcessor(
      BatchSpanProcessor.builder(new HBaseLoggingSpanExporter()).readSystemProperties().build());
    // load the system properties
    sdkTracerProvider.updateActiveTraceConfig(
      sdkTracerProvider.getActiveTraceConfig().toBuilder().readSystemProperties().build());

    OpenTelemetry openTelemetry = OpenTelemetrySdk.builder().setTracerProvider(sdkTracerProvider)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance())).build();

    // optionally set this instance as the global instance:
    GlobalOpenTelemetry.set(openTelemetry);
  }
}

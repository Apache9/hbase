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

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A interface to initialize the OpenTelemetry trace system.
 * <p/>
 * As the hbase-server module could be depended by downstream users, according to the OpenTelemetry
 * documentation, we'd better not introduce dependencies other than opentelemetry-api to avoid mess
 * up the dependency tree of downstream users.
 * <p/>
 * But if we want to enable trace when starting master/region server, we need to depend on
 * opentelemetry-sdk and other modules to initialize the Sampler, SpanProcessor, and SpanExporter,
 * programmatically. And we need to call it when starting master/regionserver, at the very
 * beginning.
 * <p/>
 * So here want introduce this interface and use the SPI mechanism to solve the problem. In the main
 * method when starting master/regionserver, we will load the implementation of this interface. If
 * none, we just skip initialization of the trace system. And in hbase-trace-config module, we will
 * implement this interface to actually initialize the trace system, and include it in our binary
 * release. Then when starting from the command line, it will find the implementation in
 * hbase-trace-config and initialize the trace system. And for downstream users, since hbase-server
 * does not depend on opentelemetry-sdk directly, we will not mess up the dependency tree.
 */
@InterfaceAudience.Private
public interface HBaseTraceInitializer {

  public static final String ENABLED = "hbase.trace.enabled";

  default boolean isTraceEnabled(Configuration conf) {
    return conf.getBoolean(ENABLED, false);
  }

  void init(Configuration conf);
}

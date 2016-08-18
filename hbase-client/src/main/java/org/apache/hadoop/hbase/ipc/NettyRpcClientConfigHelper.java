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

import com.google.common.base.Preconditions;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Helper class for passing config to {@link NettyRpcClient}.
 * <p>
 * As hadoop Configuration can not pass an Object directly, we need to find a way to pass the
 * EventLoopGroup to NettyRpcClient if we want to use a single EventLoopGroup for the whole process.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class NettyRpcClientConfigHelper {

  public static final String EVENT_LOOP_CONFIG = "hbase.client.ipc.netty.event-loop.config";

  private static final Map<String, Pair<EventLoopGroup, Class<? extends Channel>>> EVENT_LOOP_CONFIG_MAP = new HashMap<String, Pair<EventLoopGroup, Class<? extends Channel>>>();

  /**
   * Set the EventLoopGroup and channel class for NettyRpcClient. The {@code name} will be set into
   * the {@code conf} object for referring the event loop config.
   */
  public static void setEventLoopConfig(Configuration conf, String name, EventLoopGroup group,
      Class<? extends Channel> channelClass) {
    Preconditions.checkArgument(!EVENT_LOOP_CONFIG_MAP.containsKey(name), "%s is already used",
      name);
    Preconditions.checkNotNull(group, "group is null");
    Preconditions.checkNotNull(channelClass, "channel class is null");
    conf.set(EVENT_LOOP_CONFIG, name);
    EVENT_LOOP_CONFIG_MAP.put(name,
      Pair.<EventLoopGroup, Class<? extends Channel>> newPair(group, channelClass));
  }

  public static void createEventLoopPerClient(Configuration conf) {
    conf.set(EVENT_LOOP_CONFIG, "");
  }

  static Pair<EventLoopGroup, Class<? extends Channel>> getEventLoopConfig(Configuration conf) {
    String name = conf.get(EVENT_LOOP_CONFIG);
    if (name == null) {
      return DefaultNettyEventLoopConfig.GROUP_AND_CHANNEL_CLASS;
    }
    if (StringUtils.isBlank(name)) {
      return null;
    }
    return EVENT_LOOP_CONFIG_MAP.get(name);
  }
}

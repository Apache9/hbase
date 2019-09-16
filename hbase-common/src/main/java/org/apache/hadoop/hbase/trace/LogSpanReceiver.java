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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.TimelineAnnotation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.com.google.common.util.concurrent.RateLimiter;

@InterfaceAudience.Private
public class LogSpanReceiver extends SpanReceiver {
  private static final Logger LOG = LoggerFactory.getLogger(LogSpanReceiver.class);
  private static final Logger TRACELOG = LoggerFactory.getLogger("HTrace");

  public static final String TRACE_SPAN_WARN_TIME = "hbase.trace.span.warn.time";
  public static final int DEFAULT_TRACE_SPAN_WARN_TIME = 50;
  private static final String TRACE_LOG_REQUEST_COUNT_MAX = "hbase.trace.log.rate.max";

  private long warnTime;
  private final RateLimiter limiter;
  private long ignoredSpan;
  private DateFormat df;
  private Date date;

  public LogSpanReceiver(HTraceConfiguration conf) {
    this.warnTime = conf.getInt(TRACE_SPAN_WARN_TIME, DEFAULT_TRACE_SPAN_WARN_TIME);
    int maxTraceLogCountPerSecond = conf.getInt(TRACE_LOG_REQUEST_COUNT_MAX, Integer.MAX_VALUE);
    LOG.info("configure LogSpanReceiver, warnTime={}(ms), maxTraceLogCountPerSecond={}", warnTime,
      maxTraceLogCountPerSecond);
    this.limiter = RateLimiter.create(maxTraceLogCountPerSecond);
    this.ignoredSpan = 0;
    this.df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    this.date = new Date();
  }

  private String toLogString(Span span) {
    StringBuffer buf = new StringBuffer();

    buf.append("TraceId=").append(span.getTracerId()).append(", description=")
        .append(span.getDescription()).append(", consume(ms)=").append(span.getAccumulatedMillis())
        .append(", spanId=").append(span.getSpanId()).append(", parentId=")
        .append(span.getParents().length == 0 ? "" : span.getParents()[0]).append(", start=")
        .append(formatTime(span.getStartTimeMillis())).append("\n");

    if (span.getKVAnnotations() != null && span.getKVAnnotations().size() > 0) {
      buf.append("KVAnnotations\n");
      for (Map.Entry<String, String> entry : span.getKVAnnotations().entrySet()) {
        buf.append("---> ").append(entry.getKey()).append("=").append(entry.getValue())
            .append("\n");
      }
    }

    if (span.getTimelineAnnotations() != null && span.getTimelineAnnotations().size() > 0) {
      buf.append("TimelineAnnotations \n");
      long last = span.getStartTimeMillis();
      for (TimelineAnnotation annotation : span.getTimelineAnnotations()) {
        buf.append("---> ").append(formatTime(annotation.getTime())).append(" ")
            .append(annotation.getMessage()).append(" , time from last annotation: ")
            .append(annotation.getTime() - last).append(" ms\n");
        last = annotation.getTime();
      }
      buf.append("---> ").append(formatTime(span.getStopTimeMillis()))
          .append(" time from last annotation: ").append(span.getStopTimeMillis() - last)
          .append(" ms\n");
    }
    return buf.toString();
  }

  private String formatTime(long time) {
    date.setTime(time);
    return df.format(date);
  }

  @Override
  public void receiveSpan(Span span) {
    long duration = span.getAccumulatedMillis();
    if (duration >= warnTime) {
      if (limiter.tryAcquire(0, TimeUnit.MILLISECONDS)) {
        TRACELOG.info(toLogString(span));
      } else {
        ignoredSpan++;
        if (ignoredSpan > 100) {
          TRACELOG.info("More than 100 spans are ignored to be logged for by config: "
              + TRACE_LOG_REQUEST_COUNT_MAX);
          ignoredSpan = 0;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
  }
}

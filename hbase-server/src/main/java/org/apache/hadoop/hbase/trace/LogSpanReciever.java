package org.apache.hadoop.hbase.trace;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;

import org.cliffc.high_scale_lib.Counter;

public class LogSpanReciever implements SpanReceiver {
  private static final Log LOG = LogFactory.getLog(LogSpanReciever.class);
  // compatible with 0.94 trace log4j configuration
  protected static final Log TRACELOG =
      LogFactory.getLog("org.apache.hadoop.ipc.HBaseServer.trace");
  
  // configure hbase.trace.span.warn.time in hbase-site.xml
  public static final String TRACE_SPAN_WARN_TIME = "trace.span.warn.time";
  public static final int DEFAULT_TRACE_SPAN_WARN_TIME = 50;
  private long warnTime;
  
  // Once reach this upper limit, the remaining log requests in the same second will be NOP
  // by default, it's Integer.MAX, means no tracelog rate limit.
  protected int maxTraceLogCountPerSeccond;
  protected final AtomicLong lastTick = new AtomicLong(0); // in seconds
  protected final Counter traceLogCounter = new Counter(); // trace log request counter in lastTick;
  private static final String TRACE_LOG_REQUEST_COUNT_MAX = "hbase.ipc.trace.log.request.count.max";
  
  public LogSpanReciever(HTraceConfiguration conf) {
    warnTime = conf.getInt(TRACE_SPAN_WARN_TIME, DEFAULT_TRACE_SPAN_WARN_TIME);
    maxTraceLogCountPerSeccond = conf.getInt(TRACE_LOG_REQUEST_COUNT_MAX, Integer.MAX_VALUE);
    LOG.info("configure LogSpanReciever, warnTIme=" + warnTime
        + " (ms), maxTraceLogCountPerSeccond=" + maxTraceLogCountPerSeccond);
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
  }

  protected String toLogString(Span span, long duration) {
    StringBuffer buf = new StringBuffer();
    buf.append("description=").append(span.getDescription()).append(", consume(ms)=")
        .append(duration).append("\n");
    buf.append("---> ").append("spanId=").append(span.getSpanId()).append(", parentId=")
        .append(span.getParentId()).append(", traceId=").append(span.getTraceId())
        .append(", start=").append(span.getStartTimeMillis()).append("\n");
    if (span.getKVAnnotations() != null && span.getKVAnnotations().size() > 0) {
      buf.append("---> ").append("kvAnnotations=").append(span.getKVAnnotations()).append("\n");
    }
    if (span.getTimelineAnnotations() != null && span.getTimelineAnnotations().size() > 0) {
      buf.append("---> ").append("timelineAnnotations=").append(span.getTimelineAnnotations())
          .append("\n");
    }
    return span.toString();
  }
  
  @Override
  public void receiveSpan(Span span) {
    long duration = span.getAccumulatedMillis();
    if (duration >= warnTime) {
      if (maxTraceLogCountPerSeccond == Integer.MAX_VALUE) {
        // no rate limit
        TRACELOG.info(toLogString(span, duration));
      } else {
        long currTick = System.currentTimeMillis() / 1000;
        if (lastTick.getAndSet(currTick) != currTick) {
          traceLogCounter.set(1);
          TRACELOG.info(toLogString(span, duration));
        } else {
          if (traceLogCounter.intValue() < maxTraceLogCountPerSeccond) {
            TRACELOG.info(toLogString(span, duration));
            traceLogCounter.increment();
          } else {
            //once reach the rate limit in current second, we do not need to
            //update counter or do log.info() any more.
          }
        }
      }
    }
  }
}

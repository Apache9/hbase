package org.apache.hadoop.hbase.trace;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudera.htrace.HTraceConfiguration;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;

public class LogSpanReciever implements SpanReceiver {
  private static final Log LOG = LogFactory.getLog(LogSpanReciever.class);
  // compatible with 0.94 trace log4j configuration
  protected static final Log TRACELOG =
      LogFactory.getLog("org.apache.hadoop.ipc.HBaseServer.trace");
  
  // configure hbase.trace.span.warn.time in hbase-site.xml
  public static final String TRACE_SPAN_WARN_TIME = "trace.span.warn.time";
  public static final int DEFAULT_TRACE_SPAN_WARN_TIME = 50;
  private long warnTime;
  
  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void configure(HTraceConfiguration conf) {
    warnTime = conf.getInt(TRACE_SPAN_WARN_TIME, DEFAULT_TRACE_SPAN_WARN_TIME);
    LOG.info("configure LogSpanReciever, warnTIme=" + warnTime + " (ms)");
  }

  @Override
  public void receiveSpan(Span span) {
    long duration = span.getAccumulatedMillis();
    if (duration >= warnTime) {
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
      TRACELOG.info(buf.toString());
    }
  }
}

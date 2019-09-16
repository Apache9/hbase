package org.apache.hadoop.hbase.trace;

import org.apache.htrace.core.Sampler;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class TraceSampler extends Sampler {

  @Override
  public boolean next() {
    return true;
  }

}

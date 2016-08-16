package com.wavefront.agent.histogram;

import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.TDigest;

import java.util.concurrent.ConcurrentMap;

/**
 * Dispatch task for marshalling "ripe" digests for shipment to the agent
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class Dispatcher implements Runnable {
  private final ConcurrentMap<String, AgentDigest> digests;
  private final ObjectQueue<TDigest> output;

  public Dispatcher(ConcurrentMap<String, AgentDigest> digests, ObjectQueue<TDigest> output) {
    this.digests = digests;
    this.output = output;
  }

  @Override
  public void run() {

    for (String key : digests.keySet()) {
      digests.compute(key, (k, v) -> {
        if (v==null) {
          return null;
        }
        // Remove and add to shipping queue
        if (v.getDispatchTimeMillis() < System.currentTimeMillis()) {
          output.add(v);
          return null;
        }
        return v;
      });
    }
  }
}

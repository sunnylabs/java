package com.wavefront.agent.histogram;

import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import sunnylabs.report.ReportPoint;

/**
 * Dispatch task for marshalling "ripe" digests for shipment to the agent
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class Dispatcher implements Runnable {
  private final static Logger logger = Logger.getLogger(Dispatcher.class.getCanonicalName());

  private final ConcurrentMap<Utils.HistogramKey, AgentDigest> digests;
  private final ObjectQueue<ReportPoint> output;

  public Dispatcher(ConcurrentMap<Utils.HistogramKey, AgentDigest> digests, ObjectQueue<ReportPoint> output) {
    this.digests = digests;
    this.output = output;
  }

  @Override
  public void run() {

    for (Utils.HistogramKey key : digests.keySet()) {
      digests.compute(key, (k, v) -> {
        if (v == null) {
          return null;
        }
        // Remove and add to shipping queue
        if (v.getDispatchTimeMillis() < System.currentTimeMillis()) {
          try {
            ReportPoint out = ReportPoint.newBuilder()
                .setTimestamp(k.getBinTimeMillis())
                .setMetric(k.getMetric())
                .setHost(k.getSource())
                .setAnnotations(k.getTagsAsMap())
                .setTable("dummy")
                .setValue(v.toHistogram(k.getBinDurationInMillis()))
                .build();

            output.add(out);
          } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed dispatching entry " + k, e);
          }
          return null;
        }
        return v;
      });
    }
  }
}

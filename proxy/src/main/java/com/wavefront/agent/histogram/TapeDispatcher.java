package com.wavefront.agent.histogram;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Ticker;
import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import sunnylabs.report.ReportPoint;

/**
 * Dispatch task for marshalling "ripe" digests for shipment to the agent into a (Tape) ObjectQueue
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class TapeDispatcher implements Runnable {
  private final static Logger logger = Logger.getLogger(TapeDispatcher.class.getCanonicalName());

  private final ConcurrentMap<Utils.HistogramKey, AgentDigest> digests;
  private final ObjectQueue<ReportPoint> output;
  private final Ticker ticker;

  public TapeDispatcher(ConcurrentMap<Utils.HistogramKey, AgentDigest> digests, ObjectQueue<ReportPoint> output) {
    this(digests, output, Ticker.systemTicker());
  }

  @VisibleForTesting
  TapeDispatcher(ConcurrentMap<Utils.HistogramKey, AgentDigest> digests, ObjectQueue<ReportPoint> output, Ticker ticker) {
    this.digests = digests;
    this.output = output;
    this.ticker = ticker;
  }

  @Override
  public void run() {

    for (Utils.HistogramKey key : digests.keySet()) {
      digests.compute(key, (k, v) -> {
        if (v == null) {
          return null;
        }
        // Remove and add to shipping queue
        if (v.getDispatchTimeMillis() < TimeUnit.NANOSECONDS.toMillis(ticker.read())) {
          try {
            ReportPoint out = Utils.pointFromKeyAndDigest(k, v);

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

package com.wavefront.agent.histogram.accumulator;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.Validation;
import com.wavefront.agent.histogram.Utils;
import com.wavefront.ingester.Decoder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import sunnylabs.report.ReportPoint;

/**
 * Histogram accumulation task. Parses samples in Graphite/Wavefront notation from its input queue and accumulates them
 * in histograms.
 *
 * Con
 * add a builder (or make this injectable)
 * consider supporting transforms and linePredicates
 * support metric prefix
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class AccumulationTask implements Runnable {
  private static final Logger logger = Logger.getLogger(AccumulationTask.class.getCanonicalName());

  private final ObjectQueue<String> input;
  private final ConcurrentMap<Utils.HistogramKey, AgentDigest> digests;
  private final Decoder<String> decoder;
  private final List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
  private final PointHandler blockedPointsHandler;
  private final Validation.Level validationLevel;
  private final long ttlMillis;
  private final Utils.Granularity granularity;

  // Metrics
  private final Counter histogramCounter = Metrics.newCounter(new MetricName("histogram", "", "created"));
  private final Counter accumulationCounter = Metrics.newCounter(new MetricName("histogram", "", "added"));
  private final Counter ignoredCounter = Metrics.newCounter(new MetricName("histogram", "", "ignored"));

  public AccumulationTask(ObjectQueue<String> input,
                          ConcurrentMap<Utils.HistogramKey, AgentDigest> digests,
                          Decoder<String> decoder,
                          PointHandler blockedPointsHandler,
                          Validation.Level validationLevel,
                          long ttlMillis,
                          Utils.Granularity granularity) {
    this.input = input;
    this.digests = digests;
    this.decoder = decoder;
    this.blockedPointsHandler = blockedPointsHandler;
    this.validationLevel = validationLevel;
    this.ttlMillis = ttlMillis;
    this.granularity = granularity;
  }

  @Override
  public void run() {
    String line;
    while ((line = input.peek()) != null) {
      try {
        // Ignore empty lines
        if ((line = line.trim()).isEmpty()) {
          continue;
        }

        // Parse line
        points.clear();
        try {
          decoder.decodeReportPoints(line, points, "c");
        } catch (Exception e) {
          final Throwable cause = Throwables.getRootCause(e);
          String errMsg = "WF-300 Cannot parse: \"" + line + "\", reason: \"" + e.getMessage() + "\"";
          if (cause != null && cause.getMessage() != null) {
            errMsg = errMsg + ", root cause: \"" + cause.getMessage() + "\"";
          }
          throw new IllegalArgumentException(errMsg);
        }

        // now have the point, continue like in PointHandlerImpl
        ReportPoint event = points.get(0);

        // need the granularity here
        Validation.validatePoint(
            event,
            granularity.name(),
            line,
            validationLevel);

        // Get key
        Utils.HistogramKey histogramKey = Utils.makeKey(event, granularity);
        double value = (Double) event.getValue();

        // atomic update

        digests.compute(histogramKey, (k, v) -> {
          accumulationCounter.inc();
          if (v == null) {
            histogramCounter.inc();
            AgentDigest t = new AgentDigest(System.currentTimeMillis() + ttlMillis);
            t.add(value);
            return t;
          } else {
            v.add(value);
            return v;
          }
        });
      } catch (Exception e) {
        ignoredCounter.inc();
        if (StringUtils.isNotEmpty(e.getMessage())) {
          blockedPointsHandler.handleBlockedPoint(e.getMessage());
        }
      } finally {
        // ensure progress
        input.remove();
      }
    }
  }
}



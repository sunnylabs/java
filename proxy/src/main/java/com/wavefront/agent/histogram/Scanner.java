package com.wavefront.agent.histogram;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.Validation;
import com.wavefront.ingester.Decoder;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import sunnylabs.report.ReportPoint;

/**
 * Set-up
 * Should be scheduledWithFixedDelay
 * TODO add metrics
 * TODO add a builder (or make this injectable)
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class Scanner implements Runnable {
  private static final Logger logger = Logger.getLogger(Scanner.class.getCanonicalName());

  private final ObjectQueue<String> input;
  private final ConcurrentMap<String, AgentDigest> digests;
  private final Decoder<String> decoder;
  private final List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
  private final PointHandler blockedPointsHandler;
  private final Validation.Level validationLevel;
  private final Utils.Duration duration;
  private final long ttl;

  public Scanner(ObjectQueue<String> input,
                 ConcurrentMap<String, AgentDigest> digests,
                 Decoder<String> decoder,
                 PointHandler blockedPointsHandler,
                 Validation.Level validationLevel,
                 Utils.Duration duration,
                 long ttl) {
    this.input = input;
    this.digests = digests;
    this.decoder = decoder;
    this.blockedPointsHandler = blockedPointsHandler;
    this.validationLevel = validationLevel;
    this.duration = duration;
    this.ttl = ttl;
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

        // TODO Consider supporting transforms and linePredicates

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
        // TODO support metric prefix
        ReportPoint event = points.get(0);

        // need the duration here
        Validation.validatePoint(
            event,
            duration.name(),
            line,
            validationLevel);

        // make label
        String binningLabel = Utils.getBinningLabel(event, duration);
        double value = (Double) event.getValue();

        // atomic update
        digests.compute(binningLabel, (k, v) -> {
          if (v == null) {
            AgentDigest t = new AgentDigest(System.currentTimeMillis() + ttl);
            t.add(value);
            return t;
          } else {
            v.add(value);
            return v;
          }
        });
      } catch (Exception e) {
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
// Can use simple polling... reschedule this every n millis
// Should be scheduledWithFixedDelay



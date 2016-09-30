package com.wavefront.agent;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.common.Clock;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.time.DateUtils;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

import static com.wavefront.agent.Validation.validatePoint;

/**
 * Adds all graphite strings to a working list, and batches them up on a set schedule (100ms) to be sent (through the
 * daemon's logic) up to the collector on the server side.
 */
public class PointHandlerImpl implements PointHandler {

  private static final Logger logger = Logger.getLogger(PointHandlerImpl.class.getCanonicalName());

  private final Histogram receivedPointLag;
  private final String validationLevel;
  private final String handle;

  @Nullable
  private final String prefix;

  protected final int blockedPointsPerBatch;
  protected final PostPushDataTimedTask[] sendDataTasks;

  public PointHandlerImpl(final String handle,
                          final String validationLevel,
                          final int blockedPointsPerBatch,
                          final PostPushDataTimedTask[] sendDataTasks) {
    this(handle, validationLevel, blockedPointsPerBatch, null, sendDataTasks);
  }

  public PointHandlerImpl(final String handle,
                          final String validationLevel,
                          final int blockedPointsPerBatch,
                          @Nullable final String prefix,
                          final PostPushDataTimedTask[] sendDataTasks) {
    this.validationLevel = validationLevel;
    this.handle = handle;
    this.blockedPointsPerBatch = blockedPointsPerBatch;
    this.prefix = prefix;

    this.receivedPointLag = Metrics.newHistogram(
        new MetricName("points." + handle + ".received", "", "lag"));

    this.sendDataTasks = sendDataTasks;
  }

  @Override
  public void reportPoint(ReportPoint point, String debugLine) {
    final PostPushDataTimedTask randomPostTask = getRandomPostTask();
    try {
      if (prefix != null) {
        point.setMetric(prefix + "." + point.getMetric());
      }
      validatePoint(
          point,
          "" + handle,
          debugLine,
          validationLevel == null ? null : Validation.Level.valueOf(validationLevel));

      // No validation was requested by user; send forward.
      randomPostTask.addPoint(pointToString(point));
      receivedPointLag.update(Clock.now() - point.getTimestamp());

    } catch (IllegalArgumentException e) {
      this.handleBlockedPoint(e.getMessage());
    } catch (Exception ex) {
      logger.log(Level.SEVERE, "WF-500 Uncaught exception when handling point (" + debugLine + ")", ex);
    }
  }

  @Override
  public void reportPoints(List<ReportPoint> points) {
    for (final ReportPoint point : points) {
      reportPoint(point, pointToString(point));
    }
  }

  public PostPushDataTimedTask getRandomPostTask() {
    // return the task with the lowest number of pending points and, if possible, not currently flushing to retry queue
    long min = Long.MAX_VALUE;
    PostPushDataTimedTask randomPostTask = null;
    PostPushDataTimedTask firstChoicePostTask = null;
    for (int i = 0; i < this.sendDataTasks.length; i++) {
      long pointsToSend = this.sendDataTasks[i].getNumPointsToSend();
      if (pointsToSend < min) {
        min = pointsToSend;
        randomPostTask = this.sendDataTasks[i];
        if (!this.sendDataTasks[i].getFlushingToQueueFlag()) {
          firstChoicePostTask = this.sendDataTasks[i];
        }
      }
    }
    return firstChoicePostTask == null ? randomPostTask : firstChoicePostTask;
  }

  @Override
  public void handleBlockedPoint(String pointLine) {
    final PostPushDataTimedTask randomPostTask = getRandomPostTask();
    if (randomPostTask.getBlockedSampleSize() < this.blockedPointsPerBatch) {
      randomPostTask.addBlockedSample(pointLine);
    }
    randomPostTask.incrementBlockedPoints();
  }

  private static final long MILLIS_IN_YEAR = DateUtils.MILLIS_PER_DAY * 365;


  @VisibleForTesting
  static boolean pointInRange(ReportPoint point) {
    long pointTime = point.getTimestamp();
    long rightNow = System.currentTimeMillis();

    // within 1 year ago and 1 day ahead
    return (pointTime > (rightNow - MILLIS_IN_YEAR)) && (pointTime < (rightNow + DateUtils.MILLIS_PER_DAY));
  }

  private static String pointToStringSB(ReportPoint point) {
    if (point.getValue() instanceof Double || point.getValue() instanceof Long || point.getValue() instanceof String) {
      StringBuilder sb = new StringBuilder("\"")
          .append(point.getMetric().replaceAll("\"", "\\\"")).append("\" ")
          .append(point.getValue()).append(" ")
          .append(point.getTimestamp() / 1000).append(" ")
          .append("source=\"").append(point.getHost().replaceAll("\"", "\\\"")).append("\"");
      for (Map.Entry<String, String> entry : point.getAnnotations().entrySet()) {
        sb.append(" \"").append(entry.getKey().replaceAll("\"", "\\\"")).append("\"")
            .append("=")
            .append("\"").append(entry.getValue().replaceAll("\"", "\\\"")).append("\"");
      }
      return sb.toString();
    } else if (point.getValue() instanceof sunnylabs.report.Histogram){
      sunnylabs.report.Histogram h = (sunnylabs.report.Histogram) point.getValue();

      StringBuilder sb = new StringBuilder();

      // BinType
      switch (h.getDuration()) {
        case (int) DateUtils.MILLIS_PER_MINUTE:
          sb.append("!M ");
          break;
        case (int) DateUtils.MILLIS_PER_HOUR:
          sb.append("!H ");
          break;
        case (int) DateUtils.MILLIS_PER_DAY:
          sb.append("!D ");
        default:
          throw new RuntimeException("Unexpected histogram duration " + h.getDuration());
      }

      // Timestamp
      sb.append(point.getTimestamp() / 1000).append(' ');

      // Centroids
      int numCentroids = Math.min(CollectionUtils.size(h.getBins()), CollectionUtils.size(h.getCounts()));
      for (int i=0; i<numCentroids; ++i) {
        // Count
        sb.append('#').append(h.getCounts().get(i)).append(' ');
        // Mean
        sb.append(h.getBins().get(i)).append(' ');
      }

      // Metric
      sb.append('"').append(point.getMetric().replaceAll("\"", "\\\"")).append("\" ");

      // Source
      sb.append("source=\"").append(point.getHost().replaceAll("\"", "\\\"")).append("\"");
      for (Map.Entry<String, String> entry : point.getAnnotations().entrySet()) {
        sb.append(" \"").append(entry.getKey().replaceAll("\"", "\\\"")).append("\"")
            .append("=")
            .append("\"").append(entry.getValue().replaceAll("\"", "\\\"")).append("\"");
      }

      return sb.toString();
    }
    throw new RuntimeException("Unsupported value class: " + point.getValue().getClass().getCanonicalName());
  }


  @VisibleForTesting
  static String pointToString(ReportPoint point) {
    return pointToStringSB(point);
  }
}

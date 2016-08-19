package com.wavefront.agent.histogram;

import sunnylabs.report.ReportPoint;

import static com.wavefront.agent.histogram.Utils.*;

/**
 * Shared test helpers around histograms
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public final class TestUtils {
  private TestUtils() {
    // final abstract...
  }

  public static long DEFAULT_TIME_MILLIS = 1471554059000L;

  public static HistogramKey makeKey(String metric) {
    return Utils.makeKey(
        ReportPoint.newBuilder().setMetric(metric).setTimestamp(DEFAULT_TIME_MILLIS).setValue(1D).build(),
        Granularity.MINUTE);
  }
}

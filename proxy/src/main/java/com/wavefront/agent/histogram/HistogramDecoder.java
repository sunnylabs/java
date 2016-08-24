package com.wavefront.agent.histogram;

import com.wavefront.ingester.Decoder;
import com.wavefront.ingester.IngesterFormatter;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import jersey.repackaged.com.google.common.collect.ImmutableList;
import sunnylabs.report.ReportPoint;

/**
 * Decoder that takes in histograms of the type:
 *
 * [BinType] [Timestamp] [Centroids] [Metric] [Annotations]
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class HistogramDecoder implements Decoder<String> {
  private static final Logger logger = Logger.getLogger(HistogramDecoder.class.getCanonicalName());
  private static final IngesterFormatter FORMAT = IngesterFormatter.newBuilder()
      .whiteSpace()
      .binType()
      .whiteSpace()
      .appendTimestamp()
      .adjustTimestamp()
      .whiteSpace()
      .centroids()
      .whiteSpace()
      .appendMetricName()
      .whiteSpace()
      .appendAnnotationsConsumer()
      .build();

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
    ReportPoint point = FORMAT.drive(msg, null, customerId, ImmutableList.of());
    if (point != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    logger.log(Level.WARNING, "This decoder does not support customerId extraction, ignoring " + msg);
  }
}

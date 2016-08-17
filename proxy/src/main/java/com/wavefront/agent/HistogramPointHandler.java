//package com.wavefront.agent;
//
//import com.tdunning.math.stats.AVLTreeDigest;
//import com.tdunning.math.stats.TDigest;
//import com.wavefront.common.Pair;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.BlockingQueue;
//import java.util.logging.Logger;
//
//import javax.annotation.Nullable;
//
//import sunnylabs.report.ReportPoint;
//
///**
// * Accumulates incoming points into histograms (by metric/tag)
// *
// * @author Tim Schmidt (tim@wavefront.com).
// */
//public class HistogramPointHandler implements PointHandler {
//  // TODO allow setting these by argument
//  // TODO maybe do this differently
//  // Have a thread local TDigest, push them on a priority queue for combination (with minute as priority)
//  // have a blocking consumer, pull them out and send them to the collector
//  // TODO use locks?
//  private static final double COMPRESSION = 32D;
//  private static final int QUEUE_CAPACITY = 10;
//  private static final Logger logger = Logger.getLogger(HistogramPointHandler.class.getCanonicalName());
//
//  // Need a way to hold the current minutes
//
//  /**
//   * Key for purposes of event aggregation
//   */
//  private static class BinKey {
//    private final String customer;
//    private final String metric;
//    private final String host;
//    @Nullable
//    private final Map<String, String> tags;
//
//    BinKey(ReportPoint point) {
//      customer = point.getTable();
//      metric = point.getMetric();
//      host = point.getHost();
//      tags = point.getAnnotations();
//    }
//
//    @Override
//    public boolean equals(Object o) {
//      if (this == o) return true;
//      if (o == null || getClass() != o.getClass()) return false;
//
//      HistogramKey binKey = (HistogramKey) o;
//
//      if (!customer.equals(binKey.customer)) return false;
//      if (!metric.equals(binKey.metric)) return false;
//      if (!host.equals(binKey.host)) return false;
//      return tags != null ? tags.equals(binKey.tags) : binKey.tags == null;
//
//    }
//
//    @Override
//    public int hashCode() {
//      int result = customer.hashCode();
//      result = 31 * result + metric.hashCode();
//      result = 31 * result + host.hashCode();
//      result = 31 * result + (tags != null ? tags.hashCode() : 0);
//      return result;
//    }
//  }
//
//
//  private static class Accumulator {
//    private long currentMinute = -1L;
//    private Map<BinKey, TDigest> binning;
//    // TODO How to handle overflows?
//    private BlockingQueue<Pair<Long, Map<BinKey, TDigest>>> sendQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
//
//    /**
//     * TODO Schedule this any minute
//     */
//    synchronized
//    void prepareBinning() {
//      long minute = System.currentTimeMillis() / 60000L;
//
//      if (minute != currentMinute) {
//        if (!(binning == null || binning.isEmpty())) {
//          sendQueue.add(new Pair<>(currentMinute, binning));
//        }
//
//        binning = new HashMap<>();
//        currentMinute = minute;
//      }
//    }
//
//    synchronized
//    void addSample(ReportPoint p) {
//
//      prepareBinning();
//      HistogramKey key = new HistogramKey(p);
//
//      if (!binning.containsKey(key)) {
//        binning.put(key, new AVLTreeDigest(COMPRESSION));
//      }
//
//      binning.get(key).add((Double)p.getValue());
//    }
//  }
//
//  @Override
//  public void reportPoint(ReportPoint point, String debugLine) {
//    // Validate
//    Validation.validatePoint(point, ...)
//
//
//  }
//
//  @Override
//  public void reportPoints(List<ReportPoint> points) {
//
//  }
//
//  @Override
//  public void handleBlockedPoint(String pointLine) {
//
//  }
//}

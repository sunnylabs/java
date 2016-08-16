package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Map;

import sunnylabs.report.ReportPoint;

/**
 * Helpers around histograms
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public final class Utils {
  // Must be between 20 and 1000
  public static final double COMPRESSION = 32D;
  static {
    Preconditions.checkArgument(COMPRESSION >= 20D);
    Preconditions.checkArgument(COMPRESSION <= 1000D);
  }

  private Utils() {};

  /**
   *  TODO consider using Joda time to support different timezones for day accumulation
   */
  public enum Duration {
    MIN('m', 60L),
    HOUR('h', 3600L),
    DAY('d', 86400L);

    private final char prefix;
    private final long inSeconds;

    Duration(char prefix, long inSeconds) {
      this.prefix = prefix;
      this.inSeconds = inSeconds;
    }

    String getBinId(long epochSecs) {
      return "" + prefix + (epochSecs/inSeconds);
    }
  }

  public static String getBinningLabel(ReportPoint point, Duration duration) {
    Preconditions.checkNotNull(point);
    Preconditions.checkNotNull(duration);

    // Needs to be time-bin - metric, source, [tag-k-tag-val]
    String result = duration.getBinId(point.getTimestamp()) + "-" + point.getMetric();

    if (!Strings.isNullOrEmpty(point.getHost())) {
      result += "-" + point.getHost();
    }

    if (point.getAnnotations() != null) {
      for (Map.Entry<String, String> tag : point.getAnnotations().entrySet()) {
        result += "-" + tag.getKey() + ":" + tag.getValue();
      }
    }

    return result;
  }
}

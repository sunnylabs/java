package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import sunnylabs.report.ReportPoint;

/**
 * Helpers around histograms
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public final class Utils {
  // Must be between 20 and 1000
  public static final double COMPRESSION = 20D;

  static {
    Preconditions.checkArgument(COMPRESSION >= 20D);
    Preconditions.checkArgument(COMPRESSION <= 1000D);
  }

  private Utils() {
  }

  static long binTimeSecs(@NotNull String binId) {
    Duration d = Duration.fromBinId(binId);

    if (d == null) {
      throw new IllegalArgumentException("Not a valid binId: " + binId);
    }
    try {
      long time = Long.parseLong(binId.substring(1));
      return time * d.getInMillis();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Not a valid binId: " + binId);
    }
  }

  static long durationSecs(@NotNull String binId) {
    Duration d = Duration.fromBinId(binId);
    if (d == null) {
      throw new IllegalArgumentException("Not a valid binId: " + binId);
    }

    return d.getInMillis();
  }

  /**
   * TODO consider using Joda time to support different timezones for day accumulation
   */
  public enum Duration {
    MIN('m', 60L * 1000L),
    HOUR('h', 3600L * 1000L),
    DAY('d', 86400L * 1000L);

    private final char prefix;

    private final long inMillis;

    Duration(char prefix, long inMillis) {
      this.prefix = prefix;
      this.inMillis = inMillis;
    }

    static Duration fromBinId(@NotNull String binId) {
      switch (binId.charAt(0)) {
        case 'm':
          return MIN;
        case 'h':
          return HOUR;
        case 'd':
          return DAY;
        default:
          return null;
      }
    }

    public long getInMillis() {
      return inMillis;
    }


    String getBinId(long epochMillis) {
      return "" + prefix + (epochMillis / inMillis);
    }
  }

  /**
   * Need good delimiter chars
   */
  private static final char METRIC_DELIMITER = '|';

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

  public static BinKey getBinKey(ReportPoint point, Duration duration) {
    Preconditions.checkNotNull(point);
    Preconditions.checkNotNull(duration);

    String[] annotations = null;
    if (point.getAnnotations() != null) {
      // TODO should this toLowerCase tag keys (and values)
      List<Map.Entry<String, String>> keyOrderedTags = point.getAnnotations().entrySet()
          .stream().sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey())).collect(Collectors.toList());
      annotations = new String[keyOrderedTags.size() * 2];
      for (int i = 0; i < keyOrderedTags.size(); ++i) {
        annotations[2 * i] = keyOrderedTags.get(i).getKey();
        annotations[(2 * i) + 1] = keyOrderedTags.get(i).getValue();
      }
    }

    return new BinKey(
        duration.getBinId(point.getTimestamp()),
        point.getMetric(),
        point.getHost(),
        annotations
    );
  }

  public static class BinKey {
    // NOTE: fields are not final to allow object reuse
    @NotNull
    private String binId;
    @NotNull
    private String metric;
    @Nullable
    private String source;
    @Nullable
    private String[] tags;


    private BinKey(String binId, String metric, @Nullable String source, @Nullable String[] tags) {
      this.binId = binId;
      this.metric = metric;
      this.source = source;
      this.tags = tags;
    }

    private BinKey() {
    }

    @Override
    public String toString() {
      return "BinKey{" +
          "binId='" + binId + '\'' +
          ", metric='" + metric + '\'' +
          ", source='" + source + '\'' +
          ", tags=" + Arrays.toString(tags) +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BinKey binKey = (BinKey) o;

      if (!binId.equals(binKey.binId)) return false;
      if (!metric.equals(binKey.metric)) return false;
      if (source != null ? !source.equals(binKey.source) : binKey.source != null) return false;
      // Probably incorrect - comparing Object[] arrays with Arrays.equals
      return Arrays.equals(tags, binKey.tags);

    }

    @Override
    public int hashCode() {
      int result = binId.hashCode();
      result = 31 * result + metric.hashCode();
      result = 31 * result + (source != null ? source.hashCode() : 0);
      result = 31 * result + Arrays.hashCode(tags);
      return result;
    }

    public String getBinId() {
      return binId;
    }

    public String getMetric() {
      return metric;
    }

    @Nullable
    public String getSource() {
      return source;
    }

    @Nullable
    public String[] getTags() {
      return tags;
    }

    /**
     * Unpacks tags into a map.
     */
    public Map<String, String> getTagsAsMap() {
      if (tags == null || tags.length == 0) {
        return ImmutableMap.of();
      }

      Map<String, String> annotations = new HashMap<>(tags.length / 2);
      for (int i = 0; i < tags.length - 1; i += 2) {
        annotations.put(tags[i], tags[i + 1]);
      }

      return annotations;
    }
  }

  /**
   * For now (trivial) encoding of the form short length and bytes
   * TODO use chronicle-values or at least cache the byte[]
   */
  public static class BinKeyMarshaller implements BytesReader<BinKey>, BytesWriter<BinKey>, ReadResolvable<BinKeyMarshaller> {
    private static final BinKeyMarshaller INSTANCE = new BinKeyMarshaller();

    private BinKeyMarshaller() {
    }

    public static BinKeyMarshaller get() {
      return INSTANCE;
    }

    @Override
    public BinKeyMarshaller readResolve() {
      return INSTANCE;
    }

    private static void writeString(Bytes out, String s) {
      try {
        byte[] bytes = s == null ? new byte[0] : s.getBytes("UTF-8");
        out.writeShort((short) bytes.length);
        out.write(bytes);
      } catch (UnsupportedEncodingException e) {
        // todo log
        e.printStackTrace();
      }
    }

    private static String readString(Bytes in) {
      byte[] bytes = new byte[in.readShort()];
      in.read(bytes);
      return new String(bytes);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
      // ignore, stateless
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
      // ignore, stateless
    }

    @NotNull
    @Override
    public BinKey read(Bytes in, @Nullable BinKey using) {
      if (using == null) {
        using = new BinKey();
      }
      using.binId = readString(in);
      using.metric = readString(in);
      using.source = readString(in);
      int numTags = in.readShort();
      using.tags = new String[numTags];
      for (int i = 0; i < numTags; ++i) {
        using.tags[i] = readString(in);
      }
      return using;
    }

    @Override
    public void write(Bytes out, @NotNull BinKey toWrite) {
      writeString(out, toWrite.binId);
      writeString(out, toWrite.metric);
      writeString(out, toWrite.source);
      short numTags = toWrite.tags == null ? 0 : (short) toWrite.tags.length;
      out.writeShort(numTags);
      for (short i = 0; i < numTags; ++i) {
        writeString(out, toWrite.tags[i]);
      }
    }
  }
}

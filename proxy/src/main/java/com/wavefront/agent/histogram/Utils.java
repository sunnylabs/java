package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.NotNull;

import sunnylabs.report.ReportPoint;

/**
 * Helpers around histograms
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public final class Utils {
  // Must be between 20 and 1000
  public static final double COMPRESSION = 20D;
  private static final Logger logger = Logger.getLogger(Utils.class.getCanonicalName());

  static {
    Preconditions.checkArgument(COMPRESSION >= 20D);
    Preconditions.checkArgument(COMPRESSION <= 1000D);
  }

  private Utils() {
    // No instance
  }

  /**
   * Standard supported aggregation Granularities.
   */
  public enum Granularity {
    MIN(60 * 1000),
    HOUR(60 * 60 * 1000),
    DAY(24 * 60 * 60 * 1000);

    private final int inMillis;

    Granularity(int inMillis) {
      this.inMillis = inMillis;
    }

    public int getInMillis() {
      return inMillis;
    }

    public int getBinId(long timeMillis) {
      return (int) (timeMillis / inMillis);
    }
  }

  public static HistogramKey getBinKey(ReportPoint point, Granularity granularity) {
    Preconditions.checkNotNull(point);
    Preconditions.checkNotNull(granularity);

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

    return new HistogramKey(
        (byte) granularity.ordinal(),
        granularity.getBinId(point.getTimestamp()),
        point.getMetric(),
        point.getHost(),
        annotations
    );
  }

  /**
   * Uniquely identifies a time-series - time-interval pair (=bin). These are the base units
   */
  public static class HistogramKey {
    // NOTE: fields are not final to allow object reuse
    private byte durationOrdinal;
    private int binId;
    private String metric;
    @Nullable
    private String source;
    @Nullable
    private String[] tags;


    private HistogramKey(byte durationOrdinal, int binId, @NotNull String metric, @Nullable String source, @Nullable String[] tags) {
      this.durationOrdinal = durationOrdinal;
      this.binId = binId;
      this.metric = metric;
      this.source = source;
      this.tags = tags;
    }

    private HistogramKey() {
      // For decoding
    }

    public byte getDurationOrdinal() {
      return durationOrdinal;
    }

    public int getBinId() {
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      HistogramKey histogramKey = (HistogramKey) o;

      if (durationOrdinal != histogramKey.durationOrdinal) return false;
      if (binId != histogramKey.binId) return false;
      if (!metric.equals(histogramKey.metric)) return false;
      if (source != null ? !source.equals(histogramKey.source) : histogramKey.source != null) return false;
      // Probably incorrect - comparing Object[] arrays with Arrays.equals
      return Arrays.equals(tags, histogramKey.tags);

    }

    @Override
    public int hashCode() {
      int result = (int) durationOrdinal;
      result = 31 * result + binId;
      result = 31 * result + metric.hashCode();
      result = 31 * result + (source != null ? source.hashCode() : 0);
      result = 31 * result + Arrays.hashCode(tags);
      return result;
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

    public long getBinTimeMillis() {
      return getBinDurationInMillis() * binId;
    }

    public int getBinDurationInMillis() {
      return Granularity.values()[durationOrdinal].getInMillis();
    }
  }

  /**
   * For now (trivial) encoding of the form short length and bytes
   * Consider using chronicle-values or at least a local byte[] / Stringbuffers to be a little more efficient about encodings.
   */
  public static class HistogramKeyMarshaller implements BytesReader<HistogramKey>, BytesWriter<HistogramKey>, ReadResolvable<HistogramKeyMarshaller> {
    private static final HistogramKeyMarshaller INSTANCE = new HistogramKeyMarshaller();

    private HistogramKeyMarshaller() {
    }

    public static HistogramKeyMarshaller get() {
      return INSTANCE;
    }

    @Override
    public HistogramKeyMarshaller readResolve() {
      return INSTANCE;
    }

    private static void writeString(Bytes out, String s) {
      try {
        byte[] bytes = s == null ? new byte[0] : s.getBytes("UTF-8");
        out.writeShort((short) bytes.length);
        out.write(bytes);
      } catch (UnsupportedEncodingException e) {
        logger.log(Level.SEVERE, "Likely programmer error, String to Byte encoding failed: ", e);
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
    public HistogramKey read(Bytes in, @Nullable HistogramKey using) {
      if (using == null) {
        using = new HistogramKey();
      }
      using.durationOrdinal = in.readByte();
      using.binId = in.readInt();
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
    public void write(Bytes out, @NotNull HistogramKey toWrite) {
      out.writeByte(toWrite.durationOrdinal);
      out.writeInt(toWrite.binId);
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

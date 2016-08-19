package com.wavefront.agent.histogram.accumulator;

import com.squareup.tape.InMemoryObjectQueue;
import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.Validation;
import com.wavefront.agent.histogram.TestUtils;
import com.wavefront.agent.histogram.Utils;
import com.wavefront.agent.histogram.Utils.HistogramKey;
import com.wavefront.ingester.GraphiteDecoder;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jersey.repackaged.com.google.common.collect.ImmutableList;
import sunnylabs.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;

/**
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class AccumulationTaskTest {
  ObjectQueue<List<String>> in;
  ConcurrentMap<HistogramKey, AgentDigest> out;
  List<String> badPointsOut;
  AccumulationTask subject;
  private final static long TTL = 30L;

  private String lineA = "keyA " + TestUtils.DEFAULT_VALUE + " " + TestUtils.DEFAULT_TIME_MILLIS;
  private String lineB = "keyB " + TestUtils.DEFAULT_VALUE + " " + TestUtils.DEFAULT_TIME_MILLIS;
  private String lineC = "keyC " + TestUtils.DEFAULT_VALUE + " " + TestUtils.DEFAULT_TIME_MILLIS;
  private HistogramKey keyA = TestUtils.makeKey("keyA");
  private HistogramKey keyB = TestUtils.makeKey("keyB");
  private HistogramKey keyC = TestUtils.makeKey("keyC");

  @Before
  public void setUp() throws Exception {
    in = new InMemoryObjectQueue<>();
    out = new ConcurrentHashMap<>();

    subject = new AccumulationTask(
        in,
        out,
        new GraphiteDecoder("unknown", ImmutableList.of()),
        new PointHandler() {
          @Override
          public void reportPoint(ReportPoint point, String debugLine) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void reportPoints(List<ReportPoint> points) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void handleBlockedPoint(String pointLine) {
            badPointsOut.add(pointLine);
          }
        },
        Validation.Level.NUMERIC_ONLY,
        TTL,
        Utils.Granularity.MINUTE);
  }

  @Test
  public void testSingleLine() {
    in.add(ImmutableList.of(lineA));

    subject.run();

    assertThat(out.get(keyA)).isNotNull();
    assertThat(out.get(keyA).size()).isEqualTo(1);
  }

  @Test
  public void testMultipleLines() {
    in.add(ImmutableList.of(lineA, lineB, lineC));

    subject.run();

    assertThat(out.get(keyA)).isNotNull();
    assertThat(out.get(keyA).size()).isEqualTo(1);
    assertThat(out.get(keyB)).isNotNull();
    assertThat(out.get(keyB).size()).isEqualTo(1);
    assertThat(out.get(keyC)).isNotNull();
    assertThat(out.get(keyC).size()).isEqualTo(1);
  }

  @Test
  public void testAccumulation() {
    in.add(ImmutableList.of(lineA, lineA, lineA));

    subject.run();

    assertThat(out.get(keyA)).isNotNull();
    assertThat(out.get(keyA).size()).isEqualTo(3);
  }


  // TODO Bad syntax etc.
}
package com.wavefront.agent.histogram.codec;

import com.tdunning.math.stats.AgentDigest;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Use merging digest. Make sure there it uses the small encoding (the other one has an obvious bug)
 * TODO use merging digest here for fast computations and to avoid object overhead
 * TODO Maybe create a subclass of MergingDigest with the other required fields (for now, time)
 * TODO Then use the Data Accessor model to basically just move state over to OffHeap
 *
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public final class PairCodec implements SizedReader<AgentDigest>, ReadResolvable<PairCodec> {
  static final PairCodec INSTANCE = new PairCodec();

  private PairCodec() {}


  @Override
  public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
    // stateless
  }

  @Override
  public void writeMarshallable(@NotNull WireOut wire) {
    // stateless
  }

  @Override
  public PairCodec readResolve() {
    return INSTANCE;
  }

  @NotNull
  @Override
  public AgentDigest read(Bytes in, long size, @Nullable AgentDigest using) {
    return null;
  }
}

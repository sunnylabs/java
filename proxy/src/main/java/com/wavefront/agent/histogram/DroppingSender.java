package com.wavefront.agent.histogram;

import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.TDigest;

import java.util.Random;

/**
 * Test class only... consumes TDigests and sleeps for an amount of time to simulate sending
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class DroppingSender implements Runnable {
  private final ObjectQueue<TDigest> input;
  private final Random r;

  public DroppingSender(ObjectQueue<TDigest> input) {
    this.input = input;
    r = new Random();
  }

  @Override
  public void run() {
    TDigest current;

    while ((current = input.peek()) != null) {
      input.remove();
    }

    try {
      Thread.sleep(100L + (long) (r.nextDouble() * 400D));
    } catch (InterruptedException e) {
      // eat
    }
  }
}

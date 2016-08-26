package com.wavefront.agent.histogram.tape;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.ObjectQueue;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.NotNull;

/**
 * Manages Square Tape queues used by this agent.
 *
 * TODO:
 * Cleanup
 * Use the wavefront-specfic tape fork.
 * Add load error handling
 * Add tests
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class TapeDeck<T> {
  private static final Logger logger = Logger.getLogger(TapeDeck.class.getCanonicalName());

  private final LoadingCache<File, ObjectQueue<T>> queues;

  public TapeDeck(final FileObjectQueue.Converter<T> converter) {

    queues = CacheBuilder.newBuilder().build(new CacheLoader<File, ObjectQueue<T>>() {
      @Override
      public ObjectQueue<T> load(@NotNull File file) throws Exception {

        FileObjectQueue<T> queue;

        // We need exclusive ownership of the file for this deck.
        // This is really no guarantee that we have exclusive access to the file (see e.g. goo.gl/i4S7ha)
        try {
          queue = new FileObjectQueue<>(file, converter);
          FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
          Preconditions.checkNotNull(channel.tryLock());
        } catch (Exception e) {
          // TODO on an IOException, (potentially also look for File is corrupt in the msg), move/delete file and retry
          throw e;
        }

        return new ObjectQueueWrapper<T>(queue, file.getName());
      }
    });
  }

  @Nullable
  public ObjectQueue<T> getTape(@NotNull File f) {
    try {
      return queues.get(f);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error while loading " + f, e);

      return null;
    }
  }

  /**
   * Threadsafe ObjectQueue wrapper;
   */
  private static class ObjectQueueWrapper<T> implements ObjectQueue<T> {
    private final ObjectQueue<T> backingQueue;
    private final Counter addCounter;
    private final Counter removeCounter;
    private final Counter peekCounter;

    ObjectQueueWrapper(ObjectQueue<T> backingQueue, String title) {
      this.addCounter = Metrics.newCounter(new MetricName("tape." + title, "", "add"));
      this.removeCounter = Metrics.newCounter(new MetricName("tape." + title, "", "remove"));
      this.peekCounter = Metrics.newCounter(new MetricName("tape." + title, "", "peek"));

      this.backingQueue = backingQueue;
    }

    @Override
    public int size() {
      synchronized (this) {
        return backingQueue.size();
      }
    }

    @Override
    public void add(T t) {
      addCounter.inc();
      synchronized (this) {
        backingQueue.add(t);
      }
    }

    @Override
    public T peek() {
      peekCounter.inc();
      synchronized (this) {
        return backingQueue.peek();
      }
    }

    @Override
    public void remove() {
      removeCounter.inc();
      synchronized (this) {
        backingQueue.remove();
      }
    }

    @Override
    public void setListener(Listener<T> listener) {
      synchronized (this) {
        backingQueue.setListener(listener);
      }
    }
  }
}

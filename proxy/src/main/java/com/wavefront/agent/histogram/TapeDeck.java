package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.ObjectQueue;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * Manages Square Tape queues used by this agent.
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

//        if (!file.exists()) {
//          Preconditions.checkArgument(file.createNewFile());
//        }
        FileObjectQueue<T> queue;

        // We need exclusive ownership of the file for this deck.
        // This is really no guarantee that we have exclusive access to the file (see e.g. goo.gl/i4S7ha)
        try {
          queue = new FileObjectQueue<T>(file, converter);
          FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
          Preconditions.checkNotNull(channel.tryLock());
        } catch (Exception e) {
          throw e;
        }

        return new ObjectQueueWrapper<T>(queue);
      }
    });
  }

  @Nullable
  public ObjectQueue<T> getTape(@NotNull File f) {
    try {
      return queues.get(f);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Threadsafe ObjectQueue wrapper;
   */
  private static class ObjectQueueWrapper<T> implements ObjectQueue<T> {
    private final ObjectQueue<T> backingQueue;

    ObjectQueueWrapper(ObjectQueue<T> backingQueue) {
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
      synchronized (this) {
        backingQueue.add(t);
      }
    }

    @Override
    public T peek() {
      synchronized (this) {
        return backingQueue.peek();
      }
    }

    @Override
    public void remove() {
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

package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Proxy/coordinator for loading a chronicle map across multiple threads. If a file already exists at the given location,
 * will make an attempt to load the map from the existing file.
 * TODO Error handling?/
 *
 * This is a little nasty in that in continuous operation, the initial key/value/entry sizes cannot be fixed
 * TODO the marshaller typing is quite lame
 * TODO better name
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class MapLoader<K,V, KM extends BytesReader<K> & BytesWriter<K>, VM extends SizedReader<V> & SizedWriter<V>> {
  private static final Logger logger = Logger.getLogger(MapLoader.class.getCanonicalName());

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final long entries;
  private final double avgKeySize;
  private final double avgValueSize;
  private final KM keyMarshaller;
  private final VM valueMarshaller;
  private final LoadingCache<File, ChronicleMap<K,V>> maps = CacheBuilder.newBuilder().build(new CacheLoader<File, ChronicleMap<K, V>>() {
    @Override
    public ChronicleMap<K, V> load(File file) throws Exception {
      if (file.exists()) {
        // Note: this relies on an uncorrupted header, which according to the docs would be due to a hardware error or fs bug.
        // TODO handle corrupted file (probably rename existing file and create new map)
        // TODO call this chronicle map factory?
        // TODO probably store PointShortHands as keys

        // TODO Find a way to check persisted header against, likely just cast to VanillaMap or ReplicatedMap and at least compare the key value classes
        return ChronicleMap.of(keyClass, valueClass).recoverPersistedTo(file, false);
      } else {
        return ChronicleMap.of(keyClass, valueClass)
            .keyMarshaller(keyMarshaller)
            .valueMarshaller(valueMarshaller)
            .entries(entries)
            .averageKeySize(avgKeySize)
            .averageValueSize(avgValueSize)
            .createPersistedTo(file);
      }
    }
  });

  /**
   *
   * @param keyClass
   * @param valueClass
   * @param entries
   * @param avgKeySize
   * @param avgValueSize
   * @param valueMarshaller
   */
  public MapLoader(Class<K> keyClass,
                   Class<V> valueClass,
                   long entries,
                   double avgKeySize,
                   double avgValueSize,
                   KM keyMarshaller,
                   VM valueMarshaller) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.entries = entries;
    this.avgKeySize = avgKeySize;
    this.avgValueSize = avgValueSize;
    this.keyMarshaller = keyMarshaller;
    this.valueMarshaller = valueMarshaller;
  }

  public ChronicleMap<K,V> get(File f) {
    Preconditions.checkNotNull(f);
    try {
      return maps.get(f);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Failed loading map for " + f, e);
      return null;
    }
  }
}

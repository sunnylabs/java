package com.wavefront.agent.histogram;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Proxy for loading a chronicle map used for accumulation
 * This is a little nasty in that in continuous operation, the initial key/value/entry sizes cannot be fixed
 *
 * TODO better name
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class MapLoader<K,V,M extends SizedReader<V> & SizedWriter<? super V>> {
  private static final Logger logger = Logger.getLogger(MapLoader.class.getCanonicalName());


  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final long entries;
  private final double avgKeySize;
  private final double avgValueSize;
  private final M marshaller;

  // TODO add a better mechanism for specifying encoders
//  private final SizedReader<K> keyReader;
//  private final DataAccess<K> keyAccess;
//  private final
//  private final SizedReader<V> valueSizedReader;
//  private final SizedWriter<V> valueSizedWriter;

  private final LoadingCache<File, ChronicleMap<K,V>> maps = CacheBuilder.newBuilder().build(new CacheLoader<File, ChronicleMap<K, V>>() {
    @Override
    public ChronicleMap<K, V> load(File file) throws Exception {
      if (file.exists()) {
        // Note: this relies on an uncorrupted header, which according to the docs would be due to a hardware error or fs bug.
        return ChronicleMap.of(keyClass, valueClass).recoverPersistedTo(file, false);
      } else {
        return ChronicleMap.of(keyClass, valueClass)
            .valueMarshaller(marshaller)
            .entries(entries)
            .averageKeySize(avgKeySize)
            .averageValueSize(avgValueSize)
            .createPersistedTo(file);
      }
    }
  });

  public MapLoader(Class<K> keyClass,
                   Class<V> valueClass,
                   long entries,
                   double avgKeySize,
                   double avgValueSize,
                   M marshaller) {
//                   SizedWriter<V> valueAccess) {
//                   SizedReader<K> keyReader,
//                   DataAccess<K> keyAccess,
//                   SizedReader<V> valueReader,
    this.keyClass = (Class<K>) keyClass;
    this.valueClass = valueClass;
    this.entries = entries;
    this.avgKeySize = avgKeySize;
    this.avgValueSize = avgValueSize;
    this.marshaller = marshaller;
//    this.keyReader = keyReader;
//    this.keyAccess = keyAccess;
//    this.valueSizedReader = valueReader;
//    this.valueSizedWriter = valueAccess;
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

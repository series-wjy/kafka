/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

<<<<<<< HEAD
import org.apache.kafka.common.serialization.Serde;
=======
import org.apache.kafka.common.utils.Bytes;
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;
<<<<<<< HEAD
import java.util.NavigableSet;
import java.util.TreeSet;

public class MemoryNavigableLRUCache<K, V> extends MemoryLRUCache<K, V> {

    public MemoryNavigableLRUCache(String name, final int maxCacheSize, Serde<K> keySerde, Serde<V> valueSerde) {
        super(keySerde, valueSerde);

        this.name = name;
        this.keys = new TreeSet<>();

        // leave room for one extra entry to handle adding an entry before the oldest can be removed
        this.map = new LinkedHashMap<K, V>(maxCacheSize + 1, 1.01f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                if (size() > maxCacheSize) {
                    K key = eldest.getKey();
                    keys.remove(key);
                    if (listener != null) listener.apply(key, eldest.getValue());
                    return true;
                }
                return false;
            }
        };
=======
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryNavigableLRUCache extends MemoryLRUCache {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryNavigableLRUCache.class);

    public MemoryNavigableLRUCache(final String name, final int maxCacheSize) {
        super(name, maxCacheSize);
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {

        if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
        return new DelegatingPeekingKeyValueIterator<>(name(),
            new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet()
                .subSet(from, true, to, true).iterator(), treeMap));
    }

    @Override
<<<<<<< HEAD
    public KeyValueIterator<K, V> range(K from, K to) {
        return new MemoryNavigableLRUCache.CacheIterator<>(((NavigableSet<K>) this.keys).subSet(from, true, to, false).iterator(), this.map);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new MemoryNavigableLRUCache.CacheIterator<>(this.keys.iterator(), this.map);
=======
    public  KeyValueIterator<Bytes, byte[]> all() {
        final TreeMap<Bytes, byte[]> treeMap = toTreeMap();
        return new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet().iterator(), treeMap);
    }

    private synchronized TreeMap<Bytes, byte[]> toTreeMap() {
        return new TreeMap<>(this.map);
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
    }


    private static class CacheIterator implements KeyValueIterator<Bytes, byte[]> {
        private final Iterator<Bytes> keys;
        private final Map<Bytes, byte[]> entries;
        private Bytes lastKey;

        private CacheIterator(final Iterator<Bytes> keys, final Map<Bytes, byte[]> entries) {
            this.keys = keys;
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return keys.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            lastKey = keys.next();
            return new KeyValue<>(lastKey, entries.get(lastKey));
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public Bytes peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey not supported");
        }
    }
}

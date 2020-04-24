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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An in-memory LRU cache store based on HashSet and HashMap.
 */
public class MemoryLRUCache implements KeyValueStore<Bytes, byte[]> {

    public interface EldestEntryRemovalListener {
        void apply(Bytes key, byte[] value);
    }

<<<<<<< HEAD
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    protected String name;
    protected Map<K, V> map;
    protected Set<K> keys;
    private StateSerdes<K, V> serdes;
=======
    private final String name;
    protected final Map<Bytes, byte[]> map;
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

    private boolean restoring = false; // TODO: this is a sub-optimal solution to avoid logging during restoration.
                                       // in the future we should augment the StateRestoreCallback with onComplete etc to better resolve this.
    private volatile boolean open = true;

<<<<<<< HEAD
    // this is used for extended MemoryNavigableLRUCache only
    public MemoryLRUCache(Serde<K> keySerde, Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public MemoryLRUCache(String name, final int maxCacheSize, Serde<K> keySerde, Serde<V> valueSerde) {
        this(keySerde, valueSerde);

=======
    private EldestEntryRemovalListener listener;

    MemoryLRUCache(final String name, final int maxCacheSize) {
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        this.name = name;

        // leave room for one extra entry to handle adding an entry before the oldest can be removed
        this.map = new LinkedHashMap<Bytes, byte[]>(maxCacheSize + 1, 1.01f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(final Map.Entry<Bytes, byte[]> eldest) {
                final boolean evict = super.size() > maxCacheSize;
                if (evict && !restoring && listener != null) {
                    listener.apply(eldest.getKey(), eldest.getValue());
                }
                return evict;
            }
        };
    }

<<<<<<< HEAD
    public KeyValueStore<K, V> enableLogging() {
        return new InMemoryKeyValueLoggedStore<>(this.name, this, keySerde, valueSerde);
    }

    public MemoryLRUCache<K, V> whenEldestRemoved(EldestEntryRemovalListener<K, V> listener) {
=======
    void setWhenEldestRemoved(final EldestEntryRemovalListener listener) {
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        this.listener = listener;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
<<<<<<< HEAD
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        // construct the serde
        this.serdes = new StateSerdes<>(name,
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        // register the store
        context.register(root, true, new StateRestoreCallback() {
            @Override
            public void restore(byte[] key, byte[] value) {
                // check value for null, to avoid  deserialization error.
                if (value == null) {
                    put(serdes.keyFrom(key), null);
                } else {
                    put(serdes.keyFrom(key), serdes.valueFrom(value));
                }
            }
=======
    public void init(final ProcessorContext context, final StateStore root) {

        // register the store
        context.register(root, (key, value) -> {
            restoring = true;
            put(Bytes.wrap(key), value);
            restoring = false;
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        });
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized byte[] get(final Bytes key) {
        Objects.requireNonNull(key);

        return this.map.get(key);
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
        Objects.requireNonNull(key);
        if (value == null) {
            delete(key);
        } else {
            this.map.put(key, value);
        }
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key, final byte[] value) {
        Objects.requireNonNull(key);
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        Objects.requireNonNull(key);
        return this.map.remove(key);
    }

    /**
     * @throws UnsupportedOperationException at every invocation
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        throw new UnsupportedOperationException("MemoryLRUCache does not support range() function.");
    }

    /**
     * @throws UnsupportedOperationException at every invocation
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        throw new UnsupportedOperationException("MemoryLRUCache does not support all() function.");
    }

    @Override
    public long approximateNumEntries() {
        return this.map.size();
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        open = false;
    }

    public int size() {
        return this.map.size();
    }
}

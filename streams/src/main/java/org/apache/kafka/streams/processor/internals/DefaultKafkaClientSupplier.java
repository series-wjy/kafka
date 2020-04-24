<<<<<<< HEAD
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
=======
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
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
<<<<<<< HEAD

=======
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
package org.apache.kafka.streams.processor.internals;

import java.util.Map;

<<<<<<< HEAD
=======
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;

public class DefaultKafkaClientSupplier implements KafkaClientSupplier {
<<<<<<< HEAD
    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
=======
    @Deprecated
    @Override
    public AdminClient getAdminClient(final Map<String, Object> config) {
        return (AdminClient) getAdmin(config);
    }

    @Override
    public Admin getAdmin(final Map<String, Object> config) {
        // create a new client upon each call; but expect this call to be only triggered once so this should be fine
        return Admin.create(config);
    }

    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Override
<<<<<<< HEAD
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
=======
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
<<<<<<< HEAD
    public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
=======
    public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
}

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
package org.apache.kafka.streams.integration;

import kafka.log.LogConfig;
import kafka.utils.MockTime;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForCompletion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to internal topics in streams
 */
@Category({IntegrationTest.class})
public class InternalTopicIntegrationTest {
    @ClassRule
<<<<<<< HEAD
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
=======
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String APP_ID = "internal-topics-integration-test";
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";

    private final MockTime mockTime = CLUSTER.time;

    private Properties streamsProp;

    @BeforeClass
<<<<<<< HEAD
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(DEFAULT_INPUT_TOPIC);
        CLUSTER.createTopic(DEFAULT_OUTPUT_TOPIC);
    }

    /**
     * Validates that any state changelog topics are compacted
     * @return true if topics have a valid config, false otherwise
     */
    private boolean isUsingCompactionForStateChangelogTopics() {
        boolean valid = true;

        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        ZkClient zkClient = new ZkClient(
            CLUSTER.zKConnectString(),
            DEFAULT_ZK_SESSION_TIMEOUT_MS,
            DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
            ZKStringSerializer$.MODULE$);
        boolean isSecure = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(CLUSTER.zKConnectString()), isSecure);

        Map<String, Properties> topicConfigs = AdminUtils.fetchAllTopicConfigs(zkUtils);
        Iterator it = topicConfigs.iterator();
        while (it.hasNext()) {
            Tuple2<String, Properties> topicConfig = (Tuple2<String, Properties>) it.next();
            String topic = topicConfig._1;
            Properties prop = topicConfig._2;

            // state changelogs should be compacted
            if (topic.endsWith(ProcessorStateManager.STATE_CHANGELOG_TOPIC_SUFFIX)) {
                if (!prop.containsKey(LogConfig.CleanupPolicyProp()) ||
                    !prop.getProperty(LogConfig.CleanupPolicyProp()).equals(LogConfig.Compact())) {
                    valid = false;
                    break;
=======
    public static void startKafkaCluster() throws InterruptedException {
        CLUSTER.createTopics(DEFAULT_INPUT_TOPIC);
    }

    @Before
    public void before() {
        streamsProp = new Properties();
        streamsProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsProp.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProp.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsProp.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsProp.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @After
    public void after() throws IOException {
        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsProp);
    }

    private void produceData(final List<String> inputValues) throws Exception {
        final Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerProp.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProp.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerProp, mockTime);
    }

    private Properties getTopicProperties(final String changelog) {
        try (final Admin adminClient = createAdminClient()) {
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, changelog);
            try {
                final Config config = adminClient.describeConfigs(Collections.singletonList(configResource)).values().get(configResource).get();
                final Properties properties = new Properties();
                for (final ConfigEntry configEntry : config.entries()) {
                    if (configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                        properties.put(configEntry.name(), configEntry.value());
                    }
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
                }
                return properties;
            } catch (final InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Admin createAdminClient() {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        return Admin.create(adminClientConfig);
    }

    @Test
    public void shouldCompactTopicsForKeyValueStoreChangelogs() throws Exception {
        final String appID = APP_ID + "-compact";
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
<<<<<<< HEAD
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "compact-topics-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/ks-internal-topic-test");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        KStream<String, Long> wordCounts = textLines
            .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                @Override
                public Iterable<String> apply(String value) {
                    return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                }
            }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
                @Override
                public KeyValue<String, String> apply(String key, String value) {
                    return new KeyValue<String, String>(value, value);
                }
            }).countByKey("Counts").toStream();
=======
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

        textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy(MockMapper.selectValueMapper())
            .count(Materialized.as("Counts"));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProp);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Arrays.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 30000);
        streams.close();

        final Properties changelogProps = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "Counts"));
        assertEquals(LogConfig.Compact(), changelogProps.getProperty(LogConfig.CleanupPolicyProp()));

        final Properties repartitionProps = getTopicProperties(appID + "-Counts-repartition");
        assertEquals(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
        assertEquals(4, repartitionProps.size());
    }

    @Test
    public void shouldCompactAndDeleteTopicsForWindowStoreChangelogs() throws Exception {
        final String appID = APP_ID + "-compact-delete";
        streamsProp.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        final int durationMs = 2000;

        textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy(MockMapper.selectValueMapper())
            .windowedBy(TimeWindows.of(ofSeconds(1L)).grace(ofMillis(0L)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("CountWindows").withRetention(ofSeconds(2L)));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProp);
        streams.start();

        // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
        // of the input data we produce below).
        Thread.sleep(5000);

        //
        // Step 2: Produce some input data to the input topic.
        //
<<<<<<< HEAD
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerConfig);
=======
        produceData(Arrays.asList("hello", "world", "world", "hello world"));
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 30000);
        streams.close();
        final Properties properties = getTopicProperties(ProcessorStateManager.storeChangelogTopic(appID, "CountWindows"));
        final List<String> policies = Arrays.asList(properties.getProperty(LogConfig.CleanupPolicyProp()).split(","));
        assertEquals(2, policies.size());
        assertTrue(policies.contains(LogConfig.Compact()));
        assertTrue(policies.contains(LogConfig.Delete()));
        // retention should be 1 day + the window duration
        final long retention = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS) + durationMs;
        assertEquals(retention, Long.parseLong(properties.getProperty(LogConfig.RetentionMsProp())));

        final Properties repartitionProps = getTopicProperties(appID + "-CountWindows-repartition");
        assertEquals(LogConfig.Delete(), repartitionProps.getProperty(LogConfig.CleanupPolicyProp()));
        assertEquals(4, repartitionProps.size());
    }
}

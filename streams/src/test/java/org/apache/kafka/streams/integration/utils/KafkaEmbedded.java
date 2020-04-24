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
package org.apache.kafka.streams.integration.utils;

<<<<<<< HEAD

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.protocol.SecurityProtocol;
=======
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
<<<<<<< HEAD
import java.util.List;
import java.util.Properties;
=======
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 * <p>
 * Requires a running ZooKeeper instance to connect to.
 */
public class KafkaEmbedded {

    private static final Logger log = LoggerFactory.getLogger(KafkaEmbedded.class);

    private static final String DEFAULT_ZK_CONNECT = "127.0.0.1:2181";

    private final Properties effectiveConfig;
    private final File logDir;
    private final TemporaryFolder tmpFolder;
    private final KafkaServer kafka;

    /**
     * Creates and starts an embedded Kafka broker.
     *
     * @param config Broker configuration settings.  Used to modify, for example, on which port the
     *               broker should listen to.  Note that you cannot change the `log.dirs` setting
     *               currently.
     */
<<<<<<< HEAD
    public KafkaEmbedded(final Properties config) throws IOException {
        this.tmpFolder = new TemporaryFolder();
        this.tmpFolder.create();
        this.logDir = this.tmpFolder.newFolder();
        this.effectiveConfig = effectiveConfigFrom(config);
        final boolean loggingEnabled = true;
        final KafkaConfig kafkaConfig = new KafkaConfig(this.effectiveConfig, loggingEnabled);
        log.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
            this.logDir, zookeeperConnect());
        this.kafka = TestUtils.createServer(kafkaConfig, SystemTime$.MODULE$);
=======
    @SuppressWarnings("WeakerAccess")
    public KafkaEmbedded(final Properties config, final MockTime time) throws IOException {
        tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        logDir = tmpFolder.newFolder();
        effectiveConfig = effectiveConfigFrom(config);
        final boolean loggingEnabled = true;
        final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled);
        log.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
            logDir, zookeeperConnect());
        kafka = TestUtils.createServer(kafkaConfig, time);
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        log.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
            brokerList(), zookeeperConnect());
    }

    /**
     * Creates the configuration for starting the Kafka broker by merging default values with
     * overwrites.
     *
     * @param initialConfig Broker configuration settings that override the default config.
     */
<<<<<<< HEAD
    private Properties effectiveConfigFrom(final Properties initialConfig) throws IOException {
=======
    private Properties effectiveConfigFrom(final Properties initialConfig) {
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        final Properties effectiveConfig = new Properties();
        effectiveConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), 0);
        effectiveConfig.put(KafkaConfig$.MODULE$.HostNameProp(), "localhost");
        effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), "9092");
        effectiveConfig.put(KafkaConfig$.MODULE$.NumPartitionsProp(), 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), 1000000);
        effectiveConfig.put(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 10000);

        effectiveConfig.putAll(initialConfig);
        effectiveConfig.setProperty(KafkaConfig$.MODULE$.LogDirProp(), this.logDir.getAbsolutePath());
        return effectiveConfig;
    }

    /**
<<<<<<< HEAD
     * This broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
=======
     * This broker's `metadata.broker.list` value.  Example: `localhost:9092`.
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
     * <p>
     * You can use this to tell Kafka producers and consumers how to connect to this instance.
     */
    @SuppressWarnings("WeakerAccess")
    public String brokerList() {
<<<<<<< HEAD
        return this.kafka.config().hostName() + ":" + this.kafka.boundPort(SecurityProtocol.PLAINTEXT);
=======
        final Object listenerConfig = effectiveConfig.get(KafkaConfig$.MODULE$.InterBrokerListenerNameProp());
        return kafka.config().hostName() + ":" + kafka.boundPort(
            new ListenerName(listenerConfig != null ? listenerConfig.toString() : "PLAINTEXT"));
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
    }


    /**
     * The ZooKeeper connection string aka `zookeeper.connect`.
     */
    @SuppressWarnings("WeakerAccess")
    public String zookeeperConnect() {
        return this.effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
    }

    @SuppressWarnings("WeakerAccess")
    public void stopAsync() {
        log.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
<<<<<<< HEAD
            brokerList(), zookeeperConnect());
        this.kafka.shutdown();
        this.kafka.awaitShutdown();
        log.debug("Removing logs.dir at {} ...", this.logDir);
        final List<String> logDirs = Collections.singletonList(this.logDir.getAbsolutePath());
        CoreUtils.delete(scala.collection.JavaConversions.asScalaBuffer(logDirs).seq());
        this.tmpFolder.delete();
=======
                  brokerList(), zookeeperConnect());
        kafka.shutdown();
    }

    @SuppressWarnings("WeakerAccess")
    public void awaitStoppedAndPurge() {
        kafka.awaitShutdown();
        log.debug("Removing log dir at {} ...", logDir);
        try {
            Utils.delete(logDir);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        tmpFolder.delete();
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        log.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
            brokerList(), zookeeperConnect());
    }

    /**
     * Create a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(final String topic) {
<<<<<<< HEAD
        createTopic(topic, 1, 1, new Properties());
=======
        createTopic(topic, 1, 1, Collections.emptyMap());
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (the partitions of) this topic.
     */
    public void createTopic(final String topic, final int partitions, final int replication) {
<<<<<<< HEAD
        createTopic(topic, partitions, replication, new Properties());
=======
        createTopic(topic, partitions, replication, Collections.emptyMap());
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (partitions of) this topic.
     * @param topicConfig Additional topic-level configuration settings.
     */
    public void createTopic(final String topic,
                            final int partitions,
                            final int replication,
<<<<<<< HEAD
                            final Properties topicConfig) {
=======
                            final Map<String, String> topicConfig) {
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
            topic, partitions, replication, topicConfig);
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        newTopic.configs(topicConfig);

        try (final Admin adminClient = createAdminClient()) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public Admin createAdminClient() {
        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());
        final Object listeners = effectiveConfig.get(KafkaConfig$.MODULE$.ListenersProp());
        if (listeners != null && listeners.toString().contains("SSL")) {
            adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, effectiveConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            adminClientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password) effectiveConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
            adminClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        }
        return Admin.create(adminClientConfig);
    }

    @SuppressWarnings("WeakerAccess")
    public void deleteTopic(final String topic) {
        log.debug("Deleting topic { name: {} }", topic);
        try (final Admin adminClient = createAdminClient()) {
            adminClient.deleteTopics(Collections.singletonList(topic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw new RuntimeException(e);
            }
        }
    }

<<<<<<< HEAD
        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        final ZkClient zkClient = new ZkClient(
            zookeeperConnect(),
            DEFAULT_ZK_SESSION_TIMEOUT_MS,
            DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
            ZKStringSerializer$.MODULE$);
        final boolean isSecure = false;
        final ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect()), isSecure);
        AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
        zkClient.close();
    }

    public void deleteTopic(final String topic) {
        log.debug("Deleting topic { name: {} }", topic);

        final ZkClient zkClient = new ZkClient(
            zookeeperConnect(),
            DEFAULT_ZK_SESSION_TIMEOUT_MS,
            DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
            ZKStringSerializer$.MODULE$);
        final boolean isSecure = false;
        final ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect()), isSecure);
        AdminUtils.deleteTopic(zkUtils, topic);
        zkClient.close();
    }

=======
    @SuppressWarnings("WeakerAccess")
    public KafkaServer kafkaServer() {
        return kafka;
    }
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
}

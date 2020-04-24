/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

<<<<<<< HEAD
import kafka.message.{ByteBufferMessageSet, LZ4CompressionCodec, Message}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors, ProtoUtils}
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
=======
import java.nio.ByteBuffer
import java.util.Properties

import kafka.log.LogConfig
import kafka.message.ZStdCompressionCodec
import kafka.metrics.KafkaYammerMetrics
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.fail

import scala.jdk.CollectionConverters._
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

/**
  * Subclasses of `BaseProduceSendRequestTest` exercise the producer and produce request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class ProduceRequestTest extends BaseRequestTest {

<<<<<<< HEAD
  @Test
  def testSimpleProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
    val messageBuffer = new ByteBufferMessageSet(new Message("value".getBytes, "key".getBytes,
      System.currentTimeMillis(), 1: Byte)).buffer
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> messageBuffer)
    val produceResponse = sendProduceRequest(leader, new ProduceRequest(-1, 3000, partitionRecords.asJava))
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.NONE.code, partitionResponse.errorCode)
    assertEquals(0, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.timestamp)
=======
  val metricsKeySet = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala

  @Test
  def testSimpleProduceRequest(): Unit = {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")

    def sendAndCheck(memoryRecords: MemoryRecords, expectedOffset: Long): ProduceResponse.PartitionResponse = {
      val topicPartition = new TopicPartition("topic", partition)
      val partitionRecords = Map(topicPartition -> memoryRecords)
      val produceResponse = sendProduceRequest(leader,
          ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
      assertEquals(1, produceResponse.responses.size)
      val (tp, partitionResponse) = produceResponse.responses.asScala.head
      assertEquals(topicPartition, tp)
      assertEquals(Errors.NONE, partitionResponse.error)
      assertEquals(expectedOffset, partitionResponse.baseOffset)
      assertEquals(-1, partitionResponse.logAppendTime)
      assertTrue(partitionResponse.recordErrors.isEmpty)
      partitionResponse
    }

    sendAndCheck(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes)), 0)

    sendAndCheck(MemoryRecords.withRecords(CompressionType.GZIP,
      new SimpleRecord(System.currentTimeMillis(), "key1".getBytes, "value1".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key2".getBytes, "value2".getBytes)), 1)
  }

  @Test
  def testProduceWithInvalidTimestamp(): Unit = {
    val topic = "topic"
    val partition = 0
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MessageTimestampDifferenceMaxMsProp, "1000")
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)

    def createRecords(magicValue: Byte, timestamp: Long, codec: CompressionType): MemoryRecords = {
      val buf = ByteBuffer.allocate(512)
      val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
      builder.appendWithOffset(0, timestamp, null, "hello".getBytes)
      builder.appendWithOffset(1, timestamp, null, "there".getBytes)
      builder.appendWithOffset(2, timestamp, null, "beautiful".getBytes)
      builder.build()
    }

    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, System.currentTimeMillis() - 1001L, CompressionType.GZIP)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> records)
    val produceResponse = sendProduceRequest(leader, ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.INVALID_TIMESTAMP, partitionResponse.error)
    // there are 3 records with InvalidTimestampException created from inner function createRecords
    assertEquals(3, partitionResponse.recordErrors.size())
    assertEquals(0, partitionResponse.recordErrors.get(0).batchIndex)
    assertEquals(1, partitionResponse.recordErrors.get(1).batchIndex)
    assertEquals(2, partitionResponse.recordErrors.get(2).batchIndex)
    for (recordError <- partitionResponse.recordErrors.asScala) {
      assertNotNull(recordError.message)
    }
    assertEquals("One or more records have been rejected due to invalid timestamp", partitionResponse.errorMessage)
  }

  @Test
  def testProduceToNonReplica(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic and find a broker which is not the leader
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, 1, servers)
    val leader = partitionToLeader(partition)
    val nonReplicaOpt = servers.find(_.config.brokerId != leader)
    assertTrue(nonReplicaOpt.isDefined)
    val nonReplicaId =  nonReplicaOpt.get.config.brokerId

    // Send the produce request to the non-replica
    val records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("key".getBytes, "value".getBytes))
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> records)
    val produceRequest = ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build()

    val produceResponse = sendProduceRequest(nonReplicaId, produceRequest)
    assertEquals(1, produceResponse.responses.size)
    assertEquals(Errors.NOT_LEADER_FOR_PARTITION, produceResponse.responses.asScala.head._2.error)
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
  }

  /* returns a pair of partition id and leader id */
  private def createTopicAndFindPartitionWithLeader(topic: String): (Int, Int) = {
<<<<<<< HEAD
    val partitionToLeader = TestUtils.createTopic(zkUtils, topic, 3, 2, servers)
    partitionToLeader.collectFirst {
      case (partition, Some(leader)) if leader != -1 => (partition, leader)
=======
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 3, 2, servers)
    partitionToLeader.collectFirst {
      case (partition, leader) if leader != -1 => (partition, leader)
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
    }.getOrElse(fail(s"No leader elected for topic $topic"))
  }

  @Test
<<<<<<< HEAD
  def testCorruptLz4ProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
    val messageBuffer = new ByteBufferMessageSet(LZ4CompressionCodec, new Message("value".getBytes, "key".getBytes,
      System.currentTimeMillis(), 1: Byte)).buffer
    // Change the lz4 checksum value so that it doesn't match the contents
    messageBuffer.array.update(40, 0)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> messageBuffer)
    val produceResponse = sendProduceRequest(leader, new ProduceRequest(-1, 3000, partitionRecords.asJava))
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.CORRUPT_MESSAGE.code, partitionResponse.errorCode)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.timestamp)
  }

  private def sendProduceRequest(leaderId: Int, request: ProduceRequest): ProduceResponse = {
    val socket = connect(s = servers.find(_.config.brokerId == leaderId).map(_.socketServer).getOrElse {
      fail(s"Could not find broker with id $leaderId")
    })
    val response = send(socket, request, ApiKeys.PRODUCE, ProtoUtils.latestVersion(ApiKeys.PRODUCE.id))
    ProduceResponse.parse(response)
=======
  def testCorruptLz4ProduceRequest(): Unit = {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
    val timestamp = 1000000
    val memoryRecords = MemoryRecords.withRecords(CompressionType.LZ4,
      new SimpleRecord(timestamp, "key".getBytes, "value".getBytes))
    // Change the lz4 checksum value (not the kafka record crc) so that it doesn't match the contents
    val lz4ChecksumOffset = 6
    memoryRecords.buffer.array.update(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + lz4ChecksumOffset, 0)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)
    val produceResponse = sendProduceRequest(leader, 
      ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.CORRUPT_MESSAGE, partitionResponse.error)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.logAppendTime)
    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicStats.InvalidMessageCrcRecordsPerSec}")), 1)
    assertTrue(TestUtils.meterCount(s"${BrokerTopicStats.InvalidMessageCrcRecordsPerSec}") > 0)
  }

  @Test
  def testZSTDProduceRequest(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic compressed with ZSTD
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.CompressionTypeProp, ZStdCompressionCodec.name)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)
    val memoryRecords = MemoryRecords.withRecords(CompressionType.ZSTD,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)

    // produce request with v7: works fine!
    val res1 = sendProduceRequest(leader,
      new ProduceRequest.Builder(7, 7, -1, 3000, partitionRecords.asJava, null).build())
    val (tp1, partitionResponse1) = res1.responses.asScala.head
    assertEquals(topicPartition, tp1)
    assertEquals(Errors.NONE, partitionResponse1.error)
    assertEquals(0, partitionResponse1.baseOffset)
    assertEquals(-1, partitionResponse1.logAppendTime)

    // produce request with v3: returns Errors.UNSUPPORTED_COMPRESSION_TYPE.
    val res2 = sendProduceRequest(leader,
      new ProduceRequest.Builder(3, 3, -1, 3000, partitionRecords.asJava, null)
        .buildUnsafe(3))
    val (tp2, partitionResponse2) = res2.responses.asScala.head
    assertEquals(topicPartition, tp2)
    assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, partitionResponse2.error)
  }

  private def sendProduceRequest(leaderId: Int, request: ProduceRequest): ProduceResponse = {
    connectAndReceive[ProduceResponse](request, destination = brokerSocketServer(leaderId))
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
  }

}

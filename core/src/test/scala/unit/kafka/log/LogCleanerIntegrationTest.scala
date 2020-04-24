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

package kafka.log

import java.io.PrintWriter

<<<<<<< HEAD
import kafka.api.{KAFKA_0_10_0_IV1, KAFKA_0_9_0}
import kafka.common.TopicAndPartition
import kafka.message._
import kafka.server.OffsetCheckpoint
import kafka.utils._
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.utils.Utils
=======
import com.yammer.metrics.core.{Gauge, MetricName}
import kafka.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, RecordBatch}
import org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
import org.junit.Assert._
import org.junit.{After, Test}

<<<<<<< HEAD
import scala.collection._
import scala.util.Random
=======
import scala.collection.{Iterable, Seq}
import scala.jdk.CollectionConverters._
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

/**
  * This is an integration test that tests the fully integrated log cleaner
  */
class LogCleanerIntegrationTest extends AbstractLogCleanerIntegrationTest with KafkaMetricsGroup {

  val codec: CompressionType = CompressionType.LZ4

  val codec = CompressionCodec.getCompressionCodec(compressionCodec)
  val time = new MockTime()
<<<<<<< HEAD
  val segmentSize = 256
  val deleteDelay = 1000
  val logName = "log"
  val logDir = TestUtils.tempDir()
  var counter = 0
  var cleaner: LogCleaner = _
  val topics = Array(TopicAndPartition("log", 0), TopicAndPartition("log", 1), TopicAndPartition("log", 2))

  @Test
  def cleanerTest() {
    val largeMessageKey = 20
    val (largeMessageValue, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, Message.MagicValue_V1)
    val maxMessageSize = largeMessageSet.sizeInBytes

    cleaner = makeCleaner(parts = 3, maxMessageSize = maxMessageSize)
    val log = cleaner.logs.get(topics(0))

    val appends = writeDups(numKeys = 100, numDups = 3, log = log, codec = codec)
    val startSize = log.size
    cleaner.startup()

    val firstDirty = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.map(_.size).sum
    assertTrue(s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize", startSize > compactedSize)

    checkLogAfterAppendingDups(log, startSize, appends)

    log.append(largeMessageSet, assignOffsets = true)
    val dups = writeDups(startKey = largeMessageKey + 1, numKeys = 100, numDups = 3, log = log, codec = codec)
    val appends2 = appends ++ Seq(largeMessageKey -> largeMessageValue) ++ dups
    val firstDirty2 = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty2)

    checkLogAfterAppendingDups(log, startSize, appends2)

    // simulate deleting a partition, by removing it from logs
    // force a checkpoint
    // and make sure its gone from checkpoint file
    cleaner.logs.remove(topics(0))
    cleaner.updateCheckpoints(logDir)
    val checkpoints = new OffsetCheckpoint(new File(logDir,cleaner.cleanerManager.offsetCheckpointFile)).read()
    // we expect partition 0 to be gone
    assertFalse(checkpoints.contains(topics(0)))
  }

  // returns (value, ByteBufferMessageSet)
  private def createLargeSingleMessageSet(key: Int, messageFormatVersion: Byte): (String, ByteBufferMessageSet) = {
    def messageValue(length: Int): String = {
      val random = new Random(0)
      new String(random.alphanumeric.take(length).toArray)
    }
    val value = messageValue(128)
    val messageSet = TestUtils.singleMessageSet(payload = value.getBytes, codec = codec, key = key.toString.getBytes,
      magicValue = messageFormatVersion)
    (value, messageSet)
  }

  @Test
  def testCleanerWithMessageFormatV0() {
    val largeMessageKey = 20
    val (largeMessageValue, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, Message.MagicValue_V0)
    val maxMessageSize = codec match {
      case NoCompressionCodec => largeMessageSet.sizeInBytes
      case _ =>
        // the broker assigns absolute offsets for message format 0 which potentially causes the compressed size to
        // increase because the broker offsets are larger than the ones assigned by the client
        // adding `5` to the message set size is good enough for this test: it covers the increased message size while
        // still being less than the overhead introduced by the conversion from message format version 0 to 1
        largeMessageSet.sizeInBytes + 5
    }

    cleaner = makeCleaner(parts = 3, maxMessageSize = maxMessageSize)

    val log = cleaner.logs.get(topics(0))
    val props = logConfigProperties(maxMessageSize)
    props.put(LogConfig.MessageFormatVersionProp, KAFKA_0_9_0.version)
    log.config = new LogConfig(props)

    val appends = writeDups(numKeys = 100, numDups = 3, log = log, codec = codec, magicValue = Message.MagicValue_V0)
    val startSize = log.size
    cleaner.startup()

    val firstDirty = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.map(_.size).sum
    assertTrue(s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize", startSize > compactedSize)

    checkLogAfterAppendingDups(log, startSize, appends)

    val appends2: Seq[(Int, String)] = {
      val dupsV0 = writeDups(numKeys = 40, numDups = 3, log = log, codec = codec, magicValue = Message.MagicValue_V0)
      log.append(largeMessageSet, assignOffsets = true)

      // also add some messages with version 1 to check that we handle mixed format versions correctly
      props.put(LogConfig.MessageFormatVersionProp, KAFKA_0_10_0_IV1.version)
      log.config = new LogConfig(props)
      val dupsV1 = writeDups(startKey = 30, numKeys = 40, numDups = 3, log = log, codec = codec, magicValue = Message.MagicValue_V1)
      appends ++ dupsV0 ++ Seq(largeMessageKey -> largeMessageValue) ++ dupsV1
    }
    val firstDirty2 = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty2)

    checkLogAfterAppendingDups(log, startSize, appends2)
  }

  private def checkLastCleaned(topic: String, partitionId: Int, firstDirty: Long) {
    // wait until cleaning up to base_offset, note that cleaning happens only when "log dirty ratio" is higher than
    // LogConfig.MinCleanableDirtyRatioProp
    cleaner.awaitCleaned(topic, partitionId, firstDirty)
    val lastCleaned = cleaner.cleanerManager.allCleanerCheckpoints.get(TopicAndPartition(topic, partitionId)).get
    assertTrue(s"log cleaner should have processed up to offset $firstDirty, but lastCleaned=$lastCleaned",
      lastCleaned >= firstDirty)
  }

  private def checkLogAfterAppendingDups(log: Log, startSize: Long, appends: Seq[(Int, String)]) {
    val read = readFromLog(log)
    assertEquals("Contents of the map shouldn't change", appends.toMap, read.toMap)
    assertTrue(startSize > log.size)
  }

  private def readFromLog(log: Log): Iterable[(Int, String)] = {

    def messageIterator(entry: MessageAndOffset): Iterator[MessageAndOffset] =
      // create single message iterator or deep iterator depending on compression codec
      if (entry.message.compressionCodec == NoCompressionCodec) Iterator(entry)
      else ByteBufferMessageSet.deepIterator(entry)

    for (segment <- log.logSegments; entry <- segment.log; messageAndOffset <- messageIterator(entry)) yield {
      val key = TestUtils.readString(messageAndOffset.message.key).toInt
      val value = TestUtils.readString(messageAndOffset.message.payload)
=======
  val topicPartitions = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))

  @After
  def cleanup(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @Test(timeout = DEFAULT_MAX_WAIT_MS)
  def testMarksPartitionsAsOfflineAndPopulatesUncleanableMetrics(): Unit = {
    val largeMessageKey = 20
    val (_, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE)
    val maxMessageSize = largeMessageSet.sizeInBytes
    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = maxMessageSize, backOffMs = 100)

    def breakPartitionLog(tp: TopicPartition): Unit = {
      val log = cleaner.logs.get(tp)
      writeDups(numKeys = 20, numDups = 3, log = log, codec = codec)

      val partitionFile = log.logSegments.last.log.file()
      val writer = new PrintWriter(partitionFile)
      writer.write("jogeajgoea")
      writer.close()

      writeDups(numKeys = 20, numDups = 3, log = log, codec = codec)
    }

    breakPartitionLog(topicPartitions(0))
    breakPartitionLog(topicPartitions(1))

    cleaner.startup()

    val log = cleaner.logs.get(topicPartitions(0))
    val log2 = cleaner.logs.get(topicPartitions(1))
    val uncleanableDirectory = log.dir.getParent
    val uncleanablePartitionsCountGauge = getGauge[Int]("uncleanable-partitions-count", uncleanableDirectory)
    val uncleanableBytesGauge = getGauge[Long]("uncleanable-bytes", uncleanableDirectory)

    TestUtils.waitUntilTrue(() => uncleanablePartitionsCountGauge.value() == 2, "There should be 2 uncleanable partitions", 2000L)
    val expectedTotalUncleanableBytes = LogCleanerManager.calculateCleanableBytes(log, 0, log.logSegments.last.baseOffset)._2 +
      LogCleanerManager.calculateCleanableBytes(log2, 0, log2.logSegments.last.baseOffset)._2
    TestUtils.waitUntilTrue(() => uncleanableBytesGauge.value() == expectedTotalUncleanableBytes,
      s"There should be $expectedTotalUncleanableBytes uncleanable bytes", 1000L)

    val uncleanablePartitions = cleaner.cleanerManager.uncleanablePartitions(uncleanableDirectory)
    assertTrue(uncleanablePartitions.contains(topicPartitions(0)))
    assertTrue(uncleanablePartitions.contains(topicPartitions(1)))
    assertFalse(uncleanablePartitions.contains(topicPartitions(2)))
  }

  private def getGauge[T](filter: MetricName => Boolean): Gauge[T] = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (k, _) => filter(k) }
      .headOption
      .getOrElse { fail(s"Unable to find metric") }
      .asInstanceOf[(Any, Gauge[Any])]
      ._2
      .asInstanceOf[Gauge[T]]
  }

  private def getGauge[T](metricName: String): Gauge[T] = {
    getGauge(mName => mName.getName.endsWith(metricName) && mName.getScope == null)
  }

  private def getGauge[T](metricName: String, metricScope: String): Gauge[T] = {
    getGauge(k => k.getName.endsWith(metricName) && k.getScope.endsWith(metricScope))
  }

  @Test
  def testMaxLogCompactionLag(): Unit = {
    val msPerHour = 60 * 60 * 1000

    val minCompactionLagMs = 1 * msPerHour
    val maxCompactionLagMs = 6 * msPerHour

    val cleanerBackOffMs = 200L
    val segmentSize = 512
    val topicPartitions = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))
    val minCleanableDirtyRatio = 1.0F

    cleaner = makeCleaner(partitions = topicPartitions,
      backOffMs = cleanerBackOffMs,
      minCompactionLagMs = minCompactionLagMs,
      segmentSize = segmentSize,
      maxCompactionLagMs= maxCompactionLagMs,
      minCleanableDirtyRatio = minCleanableDirtyRatio)
    val log = cleaner.logs.get(topicPartitions(0))

    val T0 = time.milliseconds
    writeKeyDups(numKeys = 100, numDups = 3, log, CompressionType.NONE, timestamp = T0, startValue = 0, step = 1)

    val startSizeBlock0 = log.size

    val activeSegAtT0 = log.activeSegment

    cleaner.startup()

    // advance to a time still less than maxCompactionLagMs from start
    time.sleep(maxCompactionLagMs/2)
    Thread.sleep(5 * cleanerBackOffMs) // give cleaning thread a chance to _not_ clean
    assertEquals("There should be no cleaning until the max compaction lag has passed", startSizeBlock0, log.size)

    // advance to time a bit more than one maxCompactionLagMs from start
    time.sleep(maxCompactionLagMs/2 + 1)
    val T1 = time.milliseconds

    // write the second block of data: all zero keys
    val appends1 = writeKeyDups(numKeys = 100, numDups = 1, log, CompressionType.NONE, timestamp = T1, startValue = 0, step = 0)

    // roll the active segment
    log.roll()
    val activeSegAtT1 = log.activeSegment
    val firstBlockCleanableSegmentOffset = activeSegAtT0.baseOffset

    // the first block should get cleaned
    cleaner.awaitCleaned(new TopicPartition("log", 0), firstBlockCleanableSegmentOffset)

    val read1 = readFromLog(log)
    val lastCleaned = cleaner.cleanerManager.allCleanerCheckpoints(new TopicPartition("log", 0))
    assertTrue(s"log cleaner should have processed at least to offset $firstBlockCleanableSegmentOffset, " +
      s"but lastCleaned=$lastCleaned", lastCleaned >= firstBlockCleanableSegmentOffset)

    //minCleanableDirtyRatio  will prevent second block of data from compacting
    assertNotEquals(s"log should still contain non-zero keys", appends1, read1)

    time.sleep(maxCompactionLagMs + 1)
    // the second block should get cleaned. only zero keys left
    cleaner.awaitCleaned(new TopicPartition("log", 0), activeSegAtT1.baseOffset)

    val read2 = readFromLog(log)

    assertEquals(s"log should only contains zero keys now", appends1, read2)

    val lastCleaned2 = cleaner.cleanerManager.allCleanerCheckpoints(new TopicPartition("log", 0))
    val secondBlockCleanableSegmentOffset = activeSegAtT1.baseOffset
    assertTrue(s"log cleaner should have processed at least to offset $secondBlockCleanableSegmentOffset, " +
      s"but lastCleaned=$lastCleaned2", lastCleaned2 >= secondBlockCleanableSegmentOffset)
  }

  private def readFromLog(log: Log): Iterable[(Int, Int)] = {
    for (segment <- log.logSegments; record <- segment.log.records.asScala) yield {
      val key = TestUtils.readString(record.key).toInt
      val value = TestUtils.readString(record.value).toInt
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
      key -> value
    }
  }

<<<<<<< HEAD
  private def writeDups(numKeys: Int, numDups: Int, log: Log, codec: CompressionCodec,
                        startKey: Int = 0, magicValue: Byte = Message.CurrentMagicValue): Seq[(Int, String)] = {
    for(_ <- 0 until numDups; key <- startKey until (startKey + numKeys)) yield {
      val payload = counter.toString
      log.append(TestUtils.singleMessageSet(payload = payload.toString.getBytes, codec = codec,
        key = key.toString.getBytes, magicValue = magicValue), assignOffsets = true)
      counter += 1
      (key, payload)
    }
  }
    
  @After
  def tearDown() {
    cleaner.shutdown()
    time.scheduler.shutdown()
    Utils.delete(logDir)
  }

  private def logConfigProperties(maxMessageSize: Int, minCleanableDirtyRatio: Float = 0.0F): Properties = {
    val props = new Properties()
    props.put(LogConfig.MaxMessageBytesProp, maxMessageSize: java.lang.Integer)
    props.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    props.put(LogConfig.SegmentIndexBytesProp, 100*1024: java.lang.Integer)
    props.put(LogConfig.FileDeleteDelayMsProp, deleteDelay: java.lang.Integer)
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.MinCleanableDirtyRatioProp, minCleanableDirtyRatio: java.lang.Float)
    props
  }
  
  /* create a cleaner instance and logs with the given parameters */
  private def makeCleaner(parts: Int,
                          minCleanableDirtyRatio: Float = 0.0F,
                          numThreads: Int = 1,
                          maxMessageSize: Int = 128,
                          defaultPolicy: String = "compact",
                          policyOverrides: Map[String, String] = Map()): LogCleaner = {
    
    // create partitions and add them to the pool
    val logs = new Pool[TopicAndPartition, Log]()
    for(i <- 0 until parts) {
      val dir = new File(logDir, "log-" + i)
      dir.mkdirs()

      val log = new Log(dir = dir,
                        LogConfig(logConfigProperties(maxMessageSize, minCleanableDirtyRatio)),
                        recoveryPoint = 0L,
                        scheduler = time.scheduler,
                        time = time)
      logs.put(TopicAndPartition("log", i), log)      
    }
  
    new LogCleaner(CleanerConfig(numThreads = numThreads, ioBufferSize = maxMessageSize / 2, maxMessageSize = maxMessageSize),
                   logDirs = Array(logDir),
                   logs = logs,
                   time = time)
=======
  private def writeKeyDups(numKeys: Int, numDups: Int, log: Log, codec: CompressionType, timestamp: Long, startValue: Int, step: Int): Seq[(Int, Int)] = {
    var valCounter = startValue
    for (_ <- 0 until numDups; key <- 0 until numKeys) yield {
      val curValue = valCounter
      log.appendAsLeader(TestUtils.singletonRecords(value = curValue.toString.getBytes, codec = codec,
        key = key.toString.getBytes, timestamp = timestamp), leaderEpoch = 0)
      valCounter += step
      (key, curValue)
    }
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
  }

  @Test
  def testIsThreadFailed(): Unit = {
    val metricName = "DeadThreadCount"
    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = 100000, backOffMs = 100)
    cleaner.startup()
    assertEquals(0, cleaner.deadThreadCount)
    // we simulate the unexpected error with an interrupt
    cleaner.cleaners.foreach(_.interrupt())
    // wait until interruption is propagated to all the threads
    TestUtils.waitUntilTrue(
      () => cleaner.cleaners.foldLeft(true)((result, thread) => {
        thread.isThreadFailed && result
      }), "Threads didn't terminate unexpectedly"
    )
    assertEquals(cleaner.cleaners.size, getGauge[Int](metricName).value())
    assertEquals(cleaner.cleaners.size, cleaner.deadThreadCount)
  }
}

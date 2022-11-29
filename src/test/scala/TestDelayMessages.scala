package com.github.aesteve

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TestOutputTopic, TopologyTestDriver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.util.Properties
import scala.concurrent.duration.*
import scala.util.Try
import scala.jdk.DurationConverters.*
import scala.jdk.CollectionConverters.*

class TestDelayMessages extends AnyFlatSpec, Matchers:

  private val originTopic = "origin"
  private val delayedTopic = "delayed"
  private val delay = 4.seconds


  "The records" should "be delayed properly" in {
    val props = new Properties()
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy") // we're using TopologyTestDriver there's no need for it
    val serde = Serdes.String()
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, serde.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, serde.getClass.getName)
    val topology = DelayMessages(delay, originTopic, delayedTopic).buildTopology(StreamsBuilder())
    val driver = new TopologyTestDriver(topology, props)
    val ser = serde.serializer()
    val de = serde.deserializer()
    val origin = driver.createInputTopic(originTopic, ser, ser)
    val delayed = driver.createOutputTopic(delayedTopic, de, de)
    val (rec1Key, rec1Value) = ("k1", "raw event number 1, some value 1")
    origin.pipeInput(rec1Key, rec1Value)

    // Record must not be there
    delayed.isEmpty mustBe true
    val (rec2Key, rec2Value) = ("k2", "raw event number 2, some value 2")
    origin.pipeInput(rec2Key, rec2Value)

    // Record must still not be there
    delayed.isEmpty mustBe true

    driver.advanceWallClockTime((delay + 1.second).toJava)

    delayed.isEmpty mustBe false // record 1 should be there
    val read = delayed.readRecord()
    read.key mustBe rec1Key
    read.value mustBe rec1Value

    delayed.isEmpty mustBe false // record 2 should be there
    val read2 = delayed.readRecord()
    read2.key mustBe rec2Key
    read2.value mustBe rec2Value


    delayed.isEmpty mustBe true

    // "Oracle" testing (since we know how this works behind the hood)
    // Should be removed if we change the implementation, that's not part of the contract, but part of the implementation
    val store = driver.getTimestampedKeyValueStore(DelayRecordProcessor.StoreName)
    val entries = store.all().asScala
    entries.isEmpty mustBe true
  }
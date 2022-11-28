package com.github.aesteve

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import org.apache.kafka.streams.kstream.internals.SessionWindow
import org.apache.kafka.streams.{KeyValue, StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream.{Consumed, EmitStrategy, KStream, KTable, Materialized, Produced, SessionWindows, Suppressed, TimeWindows, Windows}
import org.apache.kafka.streams.processor.{PunctuationType, RecordContext, To, api}
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext}
import org.apache.kafka.streams.state.{Stores, TimestampedWindowStore, WindowStore}
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.internals.WindowStoreBuilder

import java.time.Instant
import java.util
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.jdk.CollectionConverters.*

case class DelayMessages(delay: FiniteDuration, originTopic: String, destTopic: String):

  private val serde = Serdes.BytesSerde()

  def buildTopology(builder: StreamsBuilder): Topology =
//    builder
//      .stream(originTopic, Consumed.`with`(serde, serde))
//      .groupByKey
//      .windowedBy(
//        TimeWindows
//          .ofSizeAndGrace(delay.toJava, java.time.Duration.ZERO)
//          .advanceBy(100.millisecond.toJava)
//      )
//      .reduce((_, last) => last)
//      .suppress(Suppressed.untilWindowCloses(unbounded()))
//      .toStream
//      .map((windowedKey, value) => KeyValue(windowedKey.key, value))
//      .to(destTopic, Produced.`with`(serde, serde))
      builder
        .addStateStore(Stores.windowStoreBuilder(
          Stores.persistentWindowStore(DelayRecordProcessor.StoreName, (delay * 10).toJava, delay.toJava, false),
          serde,
          serde
        ))
        .stream(originTopic, Consumed.`with`(serde, serde))
        .process(() => new DelayRecordProcessor(delay), DelayRecordProcessor.StoreName)
        .to(destTopic, Produced.`with`(serde, serde))

      builder.build()

case class DelayRecordProcessor(delay: Duration) extends Processor[Bytes, Bytes, Bytes, Bytes]:

  private var store: WindowStore[Bytes, Bytes] = _

  override def init(context: ProcessorContext[Bytes, Bytes]): Unit =
    store = context.getStateStore(DelayRecordProcessor.StoreName).asInstanceOf[WindowStore[Bytes, Bytes]]
    context.schedule(java.time.Duration.ofMillis(10), PunctuationType.WALL_CLOCK_TIME, _timestamp => {
      val now = Instant.now
      val from = Instant.now.minusMillis(delay.toMillis)
      val eligible = store.fetchAll(from, now).asScala
      eligible.foreach(result => {
        val key: Bytes = result.key.key()
        val value: Bytes = result.value
        context.forward(new Record(key, value, now.toEpochMilli)) // FIXME: what about headers?
      })
      context.commit()
    })


  override def process(record: api.Record[Bytes, Bytes]): Unit =
    store.put(record.key, record.value, record.timestamp)

object DelayRecordProcessor:
  val StoreName = "delayed-records"
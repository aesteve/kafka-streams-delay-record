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
import org.apache.kafka.streams.state.{Stores, TimestampedKeyValueStore, TimestampedWindowStore, ValueAndTimestamp, WindowStore}
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
      builder
        .addStateStore(Stores.timestampedKeyValueStoreBuilder(
          Stores.inMemoryKeyValueStore(DelayRecordProcessor.StoreName),
          serde,
          serde
        ))
        .stream(originTopic, Consumed.`with`(serde, serde))
        .process(() => new DelayRecordProcessor(delay), DelayRecordProcessor.StoreName)
        .to(destTopic, Produced.`with`(serde, serde))

      builder.build()

case class DelayRecordProcessor(delay: FiniteDuration) extends Processor[Bytes, Bytes, Bytes, Bytes]:

  private var store: TimestampedKeyValueStore[Bytes, Bytes] = _

  override def init(context: ProcessorContext[Bytes, Bytes]): Unit =
    store = context.getStateStore(DelayRecordProcessor.StoreName).asInstanceOf[TimestampedKeyValueStore[Bytes, Bytes]]
    context.schedule(java.time.Duration.ofMillis(10), PunctuationType.WALL_CLOCK_TIME, timestamp => {
      val threshold = timestamp - delay.toMillis
      store.all()
        .asScala
        .foreach(result => {
          val key = result.key
          val payload = result.value
          if (payload.timestamp < threshold) { // record is before
            context.forward(new Record(key, payload.value, timestamp)) // FIXME: what about headers?
            store.delete(key) // remove from the store, we forwarded it
          }
        })
      context.commit()
    })


  override def process(record: api.Record[Bytes, Bytes]): Unit =
    store.put(record.key, ValueAndTimestamp.make(record.value, record.timestamp)) // park the record in the TimestampedKVStore

object DelayRecordProcessor:
  val StoreName = "delayed-records"
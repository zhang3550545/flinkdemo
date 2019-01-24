package com.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.api.scala._

object KafkaToKafkaStreaming {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val p = new Properties()
    p.setProperty("bootstrap.servers", "localhost:9092")
    p.setProperty("group.id", "test")

    val input = env.addSource(new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), p))

    input.print()

    val p2 = new Properties()
    p2.setProperty("bootstrap.servers", "localhost:9092")
    p2.setProperty("zookeeper.connect", "localhost:2181")
    p2.setProperty("group.id", "test")
    input.addSink(new FlinkKafkaProducer010[String]("test", new SimpleStringSchema(), p2))

    env.execute("KafkaToKafkaStreaming")
  }
}

package com.test.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object KafkaFlinkExactlyOnce {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val p = new Properties()
    p.setProperty("bootstrap.servers", "localhost:9092")
    p.setProperty("group.id", "test")
    val kafkaConsumer010 = new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), p)
    val streaming = env.addSource(kafkaConsumer010)

    streaming.print()

    env.execute("KafkaFlinkExactlyOnce")

  }
}

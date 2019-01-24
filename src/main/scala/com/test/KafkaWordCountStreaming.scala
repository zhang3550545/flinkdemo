package com.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object KafkaWordCountStreaming {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")
    val stream = env.addSource(new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), properties))

    stream.print()

    val result = stream.flatMap(x => x.split(","))
      .map(x => (x, 1)).keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)

    result.print()

    env.execute("KafkaWordCountStreaming")
  }
}


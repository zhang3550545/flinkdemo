package com.test

import java.util.Properties

import com.google.gson.Gson
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object KafkaJsonStreaming {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val p = new Properties()
    p.setProperty("bootstrap.servers", "localhost:9092")
    p.setProperty("group.id", "test")

    val stream = env.addSource(new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), p))

    stream.print()

    val result = stream.map { x =>
      val g = new Gson()
      val people = g.fromJson(x, classOf[People])
      people
    }

    result.print()

    env.execute("KafkaJsonStreaming")
  }
}

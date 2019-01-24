package com.test

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object KafkaToSinkStreaming {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val p = new Properties()
    p.setProperty("bootstrap.servers", "localhost:9092")
    p.setProperty("group.id", "test")
    val input = env.addSource(new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), p))

    val sink = new MysqlSink("jdbc:mysql://localhost:3306/test", "root", "root")
    input.addSink(sink)

    val hBaseSink = new HBaseSink("student", "info")
    input.addSink(hBaseSink)

    env.execute("KafkaToSinkStreaming")
  }
}

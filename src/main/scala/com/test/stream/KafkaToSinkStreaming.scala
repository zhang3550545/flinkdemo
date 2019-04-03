package com.test.stream

import java.util.Properties

import com.test.sink.{HBaseSink, MySQLSink}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object KafkaToSinkStreaming {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val p = new Properties()
    p.setProperty("bootstrap.servers", "192.168.32.157:9092")
    p.setProperty("group.id", "test")
    val input = env.addSource(new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), p))

    // 自定义MysqlSink类，将数据Sink到mysql
    val sink = new MySQLSink("jdbc:mysql://192.168.32.157:3306/test", "root", "root")
    input.addSink(sink)

    // 自定义HBaseSink类，将数据Sink到HBase
    val hBaseSink = new HBaseSink("student", "info")
    input.addSink(hBaseSink)

    env.execute("KafkaToSinkStreaming")
  }
}

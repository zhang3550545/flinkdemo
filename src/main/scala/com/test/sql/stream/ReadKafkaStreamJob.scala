package com.test.sql.stream

import java.util.Properties

import com.google.gson.Gson
import com.test.bean.Student
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object ReadKafkaStreamJob {

  def main(args: Array[String]): Unit = {

    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(sEnv)

    val p = new Properties()
    p.setProperty("bootstrap.servers", "10.105.18.175:9092")
    p.setProperty("group.id", "test")

    val ds: DataStream[Student] = sEnv.addSource[String](new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), p))
      .map(x => {
        new Gson().fromJson(x, classOf[Student])
      })

    ds.print()

    tableEnv.registerDataStream("student", ds)

    val table = tableEnv.sqlQuery("select name,sid from student")
    table.printSchema()

    tableEnv.toAppendStream[Row](table).print()

    sEnv.execute("ReadKafkaStreamJob")
  }
}

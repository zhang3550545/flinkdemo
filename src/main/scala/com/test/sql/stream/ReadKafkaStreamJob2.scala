package com.test.sql.stream

import java.util.Properties

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala._
import org.apache.flink.formats.json.JsonRowDeserializationSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object ReadKafkaStreamJob2 {

  def main(args: Array[String]): Unit = {

    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(sEnv)

    val p = new Properties()
    p.setProperty("bootstrap.servers", "10.105.18.175:9092")
    p.setProperty("group.id", "test")

    val tableSchema = new TableSchema.Builder()
      .field("name", Types.STRING)
      .field("age", Types.STRING)
      .field("sex", Types.STRING)
      .field("sid", Types.STRING)
      .build()


    val deserializationSchema = new JsonRowDeserializationSchema(
      """
        |{
        |  "type":"object",
        |  "properties":{
        |       "name":{"type":"string"},
        |       "age":{"type":"string"},
        |       "sex":{"type":"string"},
        |       "sid":{"type":"string"}
        |  }
        |}
      """.stripMargin)

    val source = new Kafka010TableSource(tableSchema, "test", p, deserializationSchema)
    tableEnv.registerTableSource("student", source)

    val table = tableEnv.sqlQuery("select * from student")

    tableEnv.toAppendStream[Row](table).print()

    sEnv.execute("ReadKafkaStreamJob")
  }
}

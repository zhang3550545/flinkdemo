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

object ReadKafkaStreamJob4 {

  def main(args: Array[String]): Unit = {

    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(sEnv)
    sEnv.setParallelism(1)

    val p = new Properties()
    p.setProperty("bootstrap.servers", "10.105.18.175:9092")
    p.setProperty("group.id", "test")

    val tableSchema = new TableSchema.Builder()
      .field("name", Types.STRING)
      .field("age", Types.BIG_DEC) // Types.DECIMAL	DECIMAL	java.math.BigDecimal
      .field("sex", Types.STRING)
      .field("sid", Types.STRING)
      .field("timestamp", Types.BIG_DEC)
      .build()


    val deserializationSchema = new JsonRowDeserializationSchema(
      """
        |{
        |  "type":"object",
        |  "properties":{
        |       "name":{"type":"string"},
        |       "age":{"type":"integer"},
        |       "sex":{"type":"string"},
        |       "sid":{"type":"string"},
        |       "timestamp":{"type":"integer"}
        |  }
        |}
      """.stripMargin)

    val source = new Kafka010TableSource(tableSchema, "test", p, deserializationSchema)
    tableEnv.registerTableSource("student", source)

    val tableSchema2 = new TableSchema.Builder()
      .field("name", Types.STRING)
      .field("age", Types.BIG_DEC) // Types.DECIMAL	DECIMAL	java.math.BigDecimal
      .field("sex", Types.STRING)
      .field("timestamp", Types.BIG_DEC)
      .build()

    val deserializationSchema2 = new JsonRowDeserializationSchema(
      """
        |{
        |  "type":"object",
        |  "properties":{
        |       "name":{"type":"string"},
        |       "age":{"type":"integer"},
        |       "sex":{"type":"string"},
        |       "timestamp":{"type":"integer"}
        |  }
        |}
      """.stripMargin)

    val source2 = new Kafka010TableSource(tableSchema2, "people", p, deserializationSchema2)
    tableEnv.registerTableSource("people", source2)

    val table = tableEnv.sqlQuery("select student.*,people.* from student inner join people on student.name = people.name")

    tableEnv.toAppendStream[Row](table).print()

    sEnv.execute("ReadKafkaStreamJob")
  }
}

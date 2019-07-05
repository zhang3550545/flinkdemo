package com.test.sql.stream

import java.util.Properties

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.types.Row

object ReadKafkaStreamJob3 {

  def main(args: Array[String]): Unit = {

    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
    sEnv.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(sEnv)

    val p = new Properties()
    p.setProperty("bootstrap.servers", "10.105.18.175:9092")
    p.setProperty("group.id", "test")

    val kafka = new Kafka().properties(p).topic("test").version("0.10")

    val schema = new Schema()
      .field("name", Types.STRING)
      .field("age", Types.STRING)
      .field("sex", Types.STRING)
      .field("sid", Types.STRING)
      .field("timestamp", Types.BIG_DEC)
      .field("rowTime", Types.SQL_TIMESTAMP).rowtime(new Rowtime().timestampsFromField("timestamp").watermarksPeriodicBounded(60000))
      .field("procTime", Types.SQL_TIMESTAMP).proctime()

    tableEnv.connect(kafka)
      .withSchema(schema)
      .withFormat(new Json().deriveSchema())
      .inAppendMode() // 还必须指定
      .registerTableSource("student")

    val table = tableEnv.sqlQuery("select * from student")

    tableEnv.toAppendStream[Row](table).print()

    sEnv.execute("ReadKafkaStreamJob")
  }
}

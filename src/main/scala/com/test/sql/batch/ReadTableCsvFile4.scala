package com.test.sql.batch

import com.test.bean.Student
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, OldCsv, Schema}
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.flink.table.api.TableSchema

/**
  * 使用 table env的connect的方式进行直接注册
  */
object ReadTableCsvFile4 {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val connectorDescriptor = new FileSystem().path("data/1.csv")

    val tableSchema = new TableSchema.Builder()
      .field("name", Types.STRING)
      .field("age", Types.INT)
      .field("sex", Types.STRING)
      .field("sid", Types.STRING)
      .build()

    val format = new OldCsv()
      .schema(tableSchema)
      .ignoreParseErrors()

    tableEnv.connect(connectorDescriptor)
      .withSchema(new Schema().schema(tableSchema))
      .withFormat(format)
      .registerTableSource("student")

    val table = tableEnv.sqlQuery("select * from student")
    table.printSchema()

    tableEnv.toDataSet[Student](table).print()
  }
}

package com.test.sql.batch

import com.test.bean.Student
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.sources.CsvTableSource

/**
  * 直接通过tableEnv的api读取数据，并注册表
  * 需要自定义Source类，Source类需要指定Schema
  */
object ReadTableCsvFile3 {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val tableEnv = BatchTableEnvironment.create(env)

    val source = new CsvTableSource.Builder()
      .path("data/1.csv")
      .field("name", Types.STRING)
      .field("age", Types.INT)
      .field("sex", Types.STRING)
      .field("sid", Types.STRING)
      .build()

    // 直接使用 注册source 表的方式
    tableEnv.registerTableSource("student", source)

    val table = tableEnv.sqlQuery("select * from student")
    table.printSchema()

    tableEnv.toDataSet[Student](table).print()
  }

}


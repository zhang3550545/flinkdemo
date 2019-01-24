package com.test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._

object ReadTableCsvFile {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.readCsvFile[Student]("/Users/zhangzhiqiang/Documents/test_project/flinkdemo/data/1.csv")

    input.print()

    tableEnv.registerDataSet("student", input)

    val result = tableEnv.sqlQuery("select * from student")

    result.printSchema()

    result.toDataSet[Student].print()
  }
}

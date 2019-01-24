package com.test

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object ReadTableCsvFile2 {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 获取table env对象
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // 读取数据
    val input = env.readCsvFile[Student]("/Users/zhangzhiqiang/Documents/test_project/flinkdemo/data/1.csv")

    input.print()

    // 将DataSet转成Table对象
    val table = tableEnv.fromDataSet(input)

    // 注册 Table
    tableEnv.registerTable("student", table)

    // sql 查询语句
    val result = tableEnv.sqlQuery("select name,age,sex from student")

    result.printSchema()

    // 将数据转化输出
    result.toDataSet[People].print()
  }
}

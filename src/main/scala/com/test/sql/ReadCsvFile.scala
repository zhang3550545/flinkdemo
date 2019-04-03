package com.test.sql

import com.test.Student
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._


object ReadCsvFile {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 直接将数据，转成Student（相当于Schema）
    val values = env.readCsvFile[Student]("/Users/zhangzhiqiang/Documents/test_project/flinkdemo/data/1.csv")
    values.print()
  }
}

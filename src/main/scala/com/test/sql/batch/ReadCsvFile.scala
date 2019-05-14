package com.test.sql.batch

import com.test.bean.Student
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object ReadCsvFile {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 直接将数据，转成Student（相当于Schema）
    val values = env.readCsvFile[Student]("data/1.csv")
    values.print()
  }
}

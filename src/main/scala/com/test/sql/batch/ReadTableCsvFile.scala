package com.test.sql.batch

import com.test.bean.Student
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._

/**
  * 使用 batch env的api 读取数据，返回DataSet对象
  * 使用 table batch env将dataset注册为table，使用 registerDataSet方法
  * 通过tableEnv.sqlQuery进行sql查询
  */
object ReadTableCsvFile {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.readCsvFile[Student]("data/1.csv")

    input.print()

    tableEnv.registerDataSet("student", input)

    val result = tableEnv.sqlQuery("select * from student")

    result.printSchema()

    tableEnv.toDataSet[Student](result).print()
  }
}

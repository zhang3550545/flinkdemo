package com.test.sql.batch

import com.test.bean.{People, Student}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._

/**
  * 使用 batch env读取文件，返回DataSet对象，将DataSet对象注册为Table，再用table env通过sql处理
  *
  * 使用fromDataSet方法将DataSet对象转成Table
  *
  * 使用registerTable 的api注册
  */
object ReadTableCsvFile2 {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 获取table env对象
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // 读取数据
    val input = env.readCsvFile[Student]("data/1.csv")

    input.print()

    // 将DataSet转成Table对象
    val table = tableEnv.fromDataSet(input)

    // 注册 Table
    tableEnv.registerTable("student", table)

    // sql 查询语句
    val result = tableEnv.sqlQuery("select name,age,sex from student")

    result.printSchema()

    // 将数据转化输出
    tableEnv.toDataSet[People](result).print()
  }
}

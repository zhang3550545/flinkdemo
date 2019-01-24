package com.test


import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object TextWindowWordCount {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度
    env.setParallelism(2)

    val values = env.readTextFile("/Users/zhangzhiqiang/Documents/test_project/flinkdemo/data")

    values.print()

    val res = values.flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    res.writeAsCsv("/Users/zhangzhiqiang/Documents/test_project/flinkdemo/data2", FileSystem.WriteMode.NO_OVERWRITE)

    env.execute()
  }
}

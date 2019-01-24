package com.test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SessionWindowing {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = List(
      ("a", 1L, 1),
      ("a", 1L, 2),
      ("b", 1L, 1),
      ("b", 1L, 3),
      ("c", 3L, 3),
      ("c", 5L, 1)
    )


    val output = env.addSource(new SourceFunction[(String, Long, Int)] {
      override def run(ctx: SourceFunction.SourceContext[(String, Long, Int)]): Unit = {

        input.foreach { value =>
          ctx.collectWithTimestamp(value, value._3)
          ctx.emitWatermark(new Watermark(value._2))
        }

        ctx.emitWatermark(new Watermark(Long.MaxValue))
      }

      override def cancel(): Unit = {

      }
    })

    val result = output.keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(1)))
      .sum(2)

    result.print()

    result.writeAsText("/Users/zhangzhiqiang/Documents/test_project/flinkdemo/data5")

    env.execute()
  }
}

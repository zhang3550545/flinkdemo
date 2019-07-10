package com.test.sql.batch


import com.test.bean.Student
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInfo, TypeInfoFactory, TypeInformation, Types}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, OldCsv, Schema}

/**
  * 使用 table env的connect的方式进行直接注册
  */
object ReadTableCsvFile5 {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val connectorDescriptor = new FileSystem().path("data/1.csv")

    val schema = new Schema()
      .field("name", Types.STRING)
      .field("age", Types.INT)
      .field("sex", Types.STRING)
      .field("sid", Types.STRING)



    val format = new Csv()
      //.schema()
      .fieldDelimiter(',')
      .allowComments()
      .ignoreParseErrors()

    tableEnv.connect(connectorDescriptor)
      .withSchema(schema)
      .withFormat(format)
      .registerTableSource("student")

    val table = tableEnv.sqlQuery("select * from student")
    table.printSchema()

    tableEnv.toDataSet[Student](table).print()
  }
}

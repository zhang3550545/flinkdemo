package com.test.sql.batch


import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

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

    val types: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT, Types.STRING, Types.STRING)
    val names: Array[String] = Array("name", "age", "sex", "sid")
    val rowInfos = new RowTypeInfo(types, names)

    // schema 有问题，运行报错
    val format = new Csv()
      .schema(rowInfos)
      .fieldDelimiter(',')
      .allowComments()
      .ignoreParseErrors()

    tableEnv.connect(connectorDescriptor)
      .withSchema(schema)
      .withFormat(format)
      .registerTableSource("student")

    val table = tableEnv.sqlQuery("select * from student")
    table.printSchema()

    tableEnv.toDataSet[Row](table).print()
  }
}

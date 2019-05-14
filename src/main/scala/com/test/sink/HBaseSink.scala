package com.test.sink

import com.google.gson.Gson
import com.test.bean.Student
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

class HBaseSink(tableName: String, family: String) extends RichSinkFunction[String] {


  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.32.157")
    conn = ConnectionFactory.createConnection(conf)
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val g = new Gson()
    val student = g.fromJson(value, classOf[Student])
    println(value)
    println(student)

    val t: Table = conn.getTable(TableName.valueOf(tableName))

    val put: Put = new Put(Bytes.toBytes(student.sid))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes(student.name))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(student.age))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"), Bytes.toBytes(student.sex))
    t.put(put)
    t.close()
  }

  override def close(): Unit = {
    super.close()
    conn.close()
  }
}

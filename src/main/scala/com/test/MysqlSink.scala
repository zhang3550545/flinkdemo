package com.test

import java.sql.{Connection, DriverManager}

import com.google.gson.Gson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MysqlSink(url: String, user: String, pwd: String) extends RichSinkFunction[String] {

  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url, user, pwd)
    conn.setAutoCommit(false)
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val g = new Gson()
    val s = g.fromJson(value, classOf[Student])
    println(value)
    val p = conn.prepareStatement("replace into student(name,age,sex,sid) values(?,?,?,?)")
    p.setString(1, s.name)
    p.setString(2, s.age.toString)
    p.setString(3, s.sex)
    p.setString(4, s.sid)
    p.execute()
    conn.commit()
  }

  override def close(): Unit = {
    super.close()
    conn.close()
  }

}

package com.test.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author zhangzhiqiang
 * @date 2019/7/16 14:09
 */
public class SqlSinkJdbcStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = new Schema()
                .field("userId", Types.STRING)
                .field("name", Types.STRING)
                .field("age", Types.STRING)
                .field("sex", Types.STRING)
                .field("createTime", Types.BIG_DEC)
                .field("updateTime", Types.BIG_DEC);

        tableEnv
                .connect(
                        new Kafka().version("0.10").topic("user").property("bootstrap.servers", "dev-hdp-2.huazhu.com:6667")
                )
                .withSchema(schema)
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                .registerTableSource("Users");

        Table table = tableEnv.sqlQuery("select userId,name,age,sex,createTime,updateTime from Users");
        DataStream<Row> result = tableEnv.toAppendStream(table, TypeInformation.of(Row.class));
        result.print();

        JDBCAppendTableSink sink = new JDBCAppendTableSinkBuilder()
                .setDBUrl("jdbc:mysql://localhost:3306/test?useSSL=false")
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("root")
                .setBatchSize(1000)
                .setQuery("REPLACE INTO user(userId,name,age,sex,createTime,updateTime) values(?,?,?,?,?,?)")
                .setParameterTypes(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.BIG_DEC, Types.BIG_DEC})
                .build();

        tableEnv.registerTableSink("Result",
                new String[]{"userId", "name", "age", "sex", "createTime", "updateTime"},
                new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.BIG_DEC, Types.BIG_DEC},
                sink);

        tableEnv.insertInto(table, "Result", new StreamQueryConfig());

        env.execute("SqlSinkJdbcStream");
    }
}
// mysql的建表语句
/*
CREATE TABLE `user` (
    `userId` varchar(10) NOT NULL,
    `name` varchar(10) DEFAULT NULL,
    `age` varchar(3) DEFAULT NULL,
    `sex` varchar(10) DEFAULT NULL,
    `createTime` varchar(20) DEFAULT NULL,
    `updateTime` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`userId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
*/

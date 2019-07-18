package com.test.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

/**
 * @author zhangzhiqiang
 * @date 2019/7/16 13:46
 */
public class SqlSinkCsvFileStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
                        new Kafka().version("0.10").topic("user").property("bootstrap.servers", "localhost:6667")
                )
                .withSchema(schema)
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                .registerTableSource("Users");

        Table table = tableEnv.sqlQuery("select userId,name,age,sex,createTime from Users");
        tableEnv.toAppendStream(table, TypeInformation.of(Row.class)).print();

        CsvTableSink sink = new CsvTableSink("data/users.csv", ",", 1, FileSystem.WriteMode.NO_OVERWRITE);

        tableEnv.registerTableSink("Result",
                new String[]{"userId", "name", "age", "sex", "createTime"},
                new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.BIG_DEC},
                sink);

        tableEnv.insertInto(table, "Result", new StreamQueryConfig());

        env.execute("SqlSinkCsvFileStream");
    }
}

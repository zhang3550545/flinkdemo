package com.test.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

/**
 * @author zhangzhiqiang
 * @date 2019/7/15 17:02
 */
public class SqlSinkFileSystemStream {
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

        TableSchema tableSchema = new TableSchema.Builder()
                .field("userId", Types.STRING)
                .field("name", Types.STRING)
                .field("age", Types.STRING)
                .field("sex", Types.STRING)
                .field("createTime", Types.BIG_DEC)
                .field("updateTime", Types.BIG_DEC)
                .build();

        Kafka kafka = new Kafka()
                .topic("user")
                .property("bootstrap.servers", "dev-hdp-2.huazhu.com:6667,dev-hdp-3.huazhu.com:6667,dev-hdp-4.huazhu.com:6667")
                .property("group.id", "test")
                .version("0.10");

        tableEnv.connect(kafka)
                .withSchema(schema)
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                .registerTableSource("Users");

        Table table = tableEnv.sqlQuery("select * from Users");

        // 输出到本地
        tableEnv.toAppendStream(table, TypeInformation.of(Row.class)).print("row:");

        FileSystem fileSystem = new FileSystem().path("data/user.csv");
        tableEnv.connect(fileSystem)
                .withSchema(schema)
                // 使用new Csv()不是很好用，schema的参数处理不好
                .withFormat(new OldCsv().schema(tableSchema).fieldDelimiter(","))
                .inAppendMode()
                .registerTableSink("Users2");

        // 插入到fs
        QueryConfig conf = new StreamQueryConfig();
        tableEnv.insertInto(table, "Users2", conf);

        env.execute("SqlSinkFileSystemStream");
    }
}

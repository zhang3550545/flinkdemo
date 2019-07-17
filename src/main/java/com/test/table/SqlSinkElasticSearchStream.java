package com.test.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author zhangzhiqiang
 * @date 2019/7/16 10:29
 */
public class SqlSinkElasticSearchStream {
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
                        new Kafka()
                                .topic("user")
                                .property("bootstrap.servers", "dev-hdp-2.huazhu.com:6667,dev-hdp-3.huazhu.com:6667,dev-hdp-4.huazhu.com:6667")
                                .property("group.id", "test")
                                .version("0.10")
                )
                .withSchema(schema)
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                .registerTableSource("Users");


        Table table = tableEnv.sqlQuery("select userId,name,age,sex,createTime,updateTime from Users");

        // print
        tableEnv.toAppendStream(table, TypeInformation.of(Row.class)).print();

        tableEnv
                .connect(
                        new Elasticsearch()
                                .version("6")
                                .host("localhost", 9200, "http")
                                .index("test")
                                .documentType("test")
                                // key之间的分隔符，默认"_"
                                .keyDelimiter("_")
                                // 如果key为null，则用"null"字符串替代
                                .keyNullLiteral("null")
                                // 失败处理策略，Fail（报错，job失败），Ignore(失败)，RetryRejected（重试），Custom（自己定义）
                                .failureHandlerIgnore()
                                // 关闭flush检测
                                .disableFlushOnCheckpoint()
                                // 为每个批量请求设置要缓冲的最大操作数
                                .bulkFlushMaxActions(20)
                                // 每个批量请求的缓冲最大值，目前仅支持 MB
                                .bulkFlushMaxSize("20 mb")
                                // 每个批量请求间隔时间
                                .bulkFlushInterval(60000L)
                                // 设置刷新批量请求时要使用的常量回退类型
                                .bulkFlushBackoffConstant()
                                // 设置刷新批量请求时每次回退尝试之间的延迟量（毫秒）
                                .bulkFlushBackoffDelay(30000L)
                                // 设置刷新批量请求时回退尝试的最大重试次数。
                                .bulkFlushBackoffMaxRetries(3)
                                // 设置刷新大容量请求时要使用的指数回退类型。
                                //.bulkFlushBackoffExponential()
                                // 设置同一请求多次重试时的最大超时（毫秒）
                                //.connectionMaxRetryTimeout(3)
                                // 向每个REST通信添加路径前缀
                                //.connectionPathPrefix("/v1")
                )
                .withSchema(schema)
                .withFormat(new Json().deriveSchema())
                .inUpsertMode()
                .registerTableSink("Result");


        tableEnv.insertInto(table, "Result", new StreamQueryConfig());

        env.execute("SqlSinkElasticSearchStream");
    }
}

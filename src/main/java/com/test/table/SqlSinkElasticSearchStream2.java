package com.test.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.util.IgnoringFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.Elasticsearch6UpsertTableSink;
import org.apache.flink.streaming.connectors.elasticsearch6.Elasticsearch6UpsertTableSinkFactory;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangzhiqiang
 * @date 2019/7/16 10:29
 */
public class SqlSinkElasticSearchStream2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        TableSchema tableSchema = new TableSchema.Builder()
                .field("userId", Types.STRING)
                .field("name", Types.STRING)
                .field("age", Types.STRING)
                .field("sex", Types.STRING)
                .field("createTime", Types.LONG)
                .field("updateTime", Types.LONG)
                .build();

        Schema schema = new Schema()
                .field("userId", Types.STRING)
                .field("name", Types.STRING)
                .field("age", Types.STRING)
                .field("sex", Types.STRING)
                .field("createTime", Types.LONG)
                .field("updateTime", Types.LONG);


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
        tableEnv.toAppendStream(table, TypeInformation.of(Row.class)).print("append");

        // 参数参考 SqlSinkElasticSearchStream
        Map<ElasticsearchUpsertTableSinkBase.SinkOption, String> map = new HashMap<>();

        map.put(ElasticsearchUpsertTableSinkBase.SinkOption.DISABLE_FLUSH_ON_CHECKPOINT, "false");
        map.put(ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_MAX_ACTIONS, "20");
        map.put(ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_MAX_SIZE, "20 mb");
        map.put(ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_INTERVAL, "60000");
        map.put(ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_DELAY, "30000");
        map.put(ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_RETRIES, "3");

        ArrayList<ElasticsearchUpsertTableSinkBase.Host> hosts = new ArrayList<>();
        ElasticsearchUpsertTableSinkBase.Host host = new ElasticsearchUpsertTableSinkBase.Host("localhost", 9200, "http");
        hosts.add(host);

        Elasticsearch6UpsertTableSink sink = new Elasticsearch6UpsertTableSink(
                true, tableSchema, hosts, "test", "test",
                "_", "null",
                // Caused by: java.lang.ClassCastException: org.apache.flink.api.java.typeutils.GenericTypeInfo
                // cannot be cast to org.apache.flink.api.java.typeutils.RowTypeInfo
                // todo 这里有问题
                new JsonRowSerializationSchema(TypeInformation.of(Row.class)),
                XContentType.JSON, new IgnoringFailureHandler(), map);

        tableEnv.writeToSink(table, sink, new StreamQueryConfig());

        env.execute("SqlSinkElasticSearchStream");
    }
}

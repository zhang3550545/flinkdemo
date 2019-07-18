package com.test.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.util.IgnoringFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.Elasticsearch6UpsertTableSink;
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
                .field("createTime", Types.BIG_DEC)
                .field("updateTime", Types.BIG_DEC)
                .build();

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
                                .property("bootstrap.servers", "localhost:9092")
                                .property("group.id", "test")
                                .version("0.10")
                )
                .withSchema(new Schema().schema(tableSchema))
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

        TypeInformation<?>[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.BIG_DEC, Types.BIG_DEC};
        String[] names = new String[]{"userId", "name", "age", "sex", "createTime", "updateTime"};
        RowTypeInfo typeInfo = new RowTypeInfo(types, names);

        SerializationSchema<Row> schemaRow = new JsonRowSerializationSchema(typeInfo);

        Elasticsearch6UpsertTableSink sink = new Elasticsearch6UpsertTableSink(
                true, tableSchema, hosts, "test", "test",
                "_", "null",
                schemaRow,
                XContentType.JSON, new IgnoringFailureHandler(), map);

        tableEnv.writeToSink(table, sink, new StreamQueryConfig());

        env.execute("SqlSinkElasticSearchStream");
    }
}

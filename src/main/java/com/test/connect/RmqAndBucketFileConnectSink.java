package com.test.connect;

import com.google.gson.Gson;
import com.test.bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.time.ZoneId;
import java.util.Properties;

/**
 * @author zhangzhiqiang
 * @date 2019/7/10 10:11
 */
public class RmqAndBucketFileConnectSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        SingleOutputStreamOperator<String> ds = env.addSource(new FlinkKafkaConsumer010<String>("user", new SimpleStringSchema(), p))
                .map(new MapFunction<String, User>() {
                    @Override
                    public User map(String value) throws Exception {
                        return new Gson().fromJson(value, User.class);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<User>() {
                    @Override
                    public long extractAscendingTimestamp(User element) {
                        return element.createTime;
                    }
                })
                .map(new MapFunction<User, String>() {
                    @Override
                    public String map(User value) throws Exception {
                        return value.userId + "," + value.name + "," + value.age + "," + value.sex + "," + value.createTime + "," + value.updateTime;
                    }
                });


        // 写入RabbitMQ
        RMQConnectionConfig rmqConnectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setVirtualHost("/")
                .setPort(5672)
                .setUserName("admin")
                .setPassword("admin")
                .build();

        // 写入RabbitMQ，如果队列是持久化的，需要重写RMQSink的 setupQueue方法
        RMQSink<String> rmqSink = new RMQSink<>(rmqConnectionConfig, "queue_name", new SimpleStringSchema());
        ds.addSink(rmqSink);


        // 写入HDFS
        BucketingSink<String> bucketingSink = new BucketingSink<>("/apps/hive/warehouse/crm_message");
        // 设置以yyyyMMdd的格式进行切分目录，类似hive的日期分区
        bucketingSink.setBucketer(new DateTimeBucketer<>("yyyyMMdd", ZoneId.of("Asia/Shanghai")));
        // 设置文件块大小128M，超过128M会关闭当前文件，开启下一个文件
        bucketingSink.setBatchSize(1024 * 1024 * 128L);
        // 设置一小时翻滚一次
        bucketingSink.setBatchRolloverInterval(60 * 60 * 1000L);
        // 设置等待写入的文件前缀，默认是_
        bucketingSink.setPendingPrefix("");
        // 设置等待写入的文件后缀，默认是.pending
        bucketingSink.setPendingSuffix("");
        //设置正在处理的文件前缀，默认为_
        bucketingSink.setInProgressPrefix(".");

        ds.addSink(bucketingSink);


        env.execute("RmqAndBucketFileConnectSink");
    }
}

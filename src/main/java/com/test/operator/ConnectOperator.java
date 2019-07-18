package com.test.operator;

import com.google.gson.Gson;
import com.test.bean.People;
import com.test.bean.Student;
import com.test.bean.Teacher;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import scala.Tuple5;

import java.util.Properties;

/**
 * connect是将2个流连接起来，可以同时对 connect起来的流进行 统一的map 或 keyBy 操作
 */
public class ConnectOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");

        SingleOutputStreamOperator<Student> student = sEnv
                .addSource(new FlinkKafkaConsumer010<String>("student", new SimpleStringSchema(), p))
                .map(new MapFunction<String, Student>() {
                    @Override
                    public Student map(String value) throws Exception {
                        return new Gson().fromJson(value, Student.class);
                    }
                });

        student.print();

        SingleOutputStreamOperator<Teacher> teacher = sEnv
                .addSource(new FlinkKafkaConsumer010<String>("teacher", new SimpleStringSchema(), p))
                .map(new MapFunction<String, Teacher>() {
                    @Override
                    public Teacher map(String value) throws Exception {
                        return new Gson().fromJson(value, Teacher.class);
                    }
                });

        teacher.print();

        ConnectedStreams<Student, Teacher> connect = student.connect(teacher);

        connect.process(new CoProcessFunction<Student, Teacher, Tuple5<String, Integer, String, String, Long>>() {
            @Override
            public void processElement1(Student value, Context ctx, Collector<Tuple5<String, Integer, String, String, Long>> out) throws Exception {
                out.collect(new Tuple5<>(value.name, value.age, value.sex, value.classId, value.timestamp));
            }

            @Override
            public void processElement2(Teacher value, Context ctx, Collector<Tuple5<String, Integer, String, String, Long>> out) throws Exception {
                out.collect(new Tuple5<>(value.name, value.age, value.sex, value.classId, value.timestamp));
            }
        }).print("process");

        // connect
        connect.map(new CoMapFunction<Student, Teacher, Tuple5<String, Integer, String, String, Long>>() {
            @Override
            public Tuple5<String, Integer, String, String, Long> map1(Student value) throws Exception {
                return new Tuple5<>(value.name, value.age, value.sex, value.classId, value.timestamp);
            }

            @Override
            public Tuple5<String, Integer, String, String, Long> map2(Teacher value) throws Exception {
                return new Tuple5<>(value.name, value.age, value.sex, value.classId, value.timestamp);
            }
        }).print("map");


        // union
        student.map(new MapFunction<Student, Tuple5<String, Integer, String, String, Long>>() {
            @Override
            public Tuple5<String, Integer, String, String, Long> map(Student value) throws Exception {
                return new Tuple5<>(value.name, value.age, value.sex, value.classId, value.timestamp);
            }
        }).union(teacher.map(new MapFunction<Teacher, Tuple5<String, Integer, String, String, Long>>() {
            @Override
            public Tuple5<String, Integer, String, String, Long> map(Teacher value) throws Exception {
                return new Tuple5<>(value.name, value.age, value.sex, value.classId, value.timestamp);
            }
        })).print("union");


        sEnv.execute("ConnectOperator");
    }
}

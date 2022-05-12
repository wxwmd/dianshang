package com.ms.shoudan;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * 使用flink cdc抽取mysql中gmall.order_info变化的数据
 * 写入kafka中
 */
public class mysqlcdckafka {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("ubuntu")
                .port(3306)
                .databaseList("gmall") // 设置数据库，可以填写多个
                .tableList("gmall.order_info") // 设置要cdc的表，可以设置多个
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(4).print();

//        DataStreamSource<String> sqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                .setParallelism(4);
//
//        sqlSource.addSink(new FlinkKafkaProducer<String>("ubuntu:9092", "order_info", new SimpleStringSchema()));

        env.execute("mysql cdc to kafka");
    }
}
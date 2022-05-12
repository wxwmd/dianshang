package com.ms.shoudan;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

/**
 * 1. 从kafka中读取订单消息
 * 2. 连接hbase查询用户是否是首单
 * 3. 将是首单的消息写入到es中，使用kibana进行分析
 */
public class FirstOrder {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","192.168.126.128:9092");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,"wxwmd");

        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<String>("order_info", new SimpleStringSchema(), kafkaProps));

        kafkaSource.map(JSONObject::parseObject)
                .map(object->object.getString("after"))
                .filter(new FirstOrderFilter())
                .print();

        env.execute("首单分析");
    }


    static class FirstOrderFilter implements FilterFunction<String>{

        public FirstOrderFilter() throws IOException {

        }

        @Override
        public boolean filter(String afterJson) throws Exception {
            System.out.println("afterJson:  "+afterJson);
            if (afterJson!=null && !afterJson.equals("")){
                JSONObject after = JSONObject.parseObject(afterJson);
                String userId = after.getString("user_id");
                Get get = new Get(userId.getBytes());

                Configuration hbaseConf = HBaseConfiguration.create();
                hbaseConf.set("hbase.zookeeper.quorum", "ubuntu:2181");
                Connection connection= ConnectionFactory.createConnection(hbaseConf);
                Table hasOrderedTable = connection.getTable(TableName.valueOf("has_ordered"));
                Result result = hasOrderedTable.get(get);

                System.out.println(result.getExists());
                System.out.println(result.isEmpty());
                // 这条数据不存在
                if (!result.getExists()){
                    Put put = new Put(userId.getBytes());
                    put.addColumn("user_id".getBytes(),"id".getBytes(),userId.getBytes());
                    put.addColumn("ordered".getBytes(),"status".getBytes(),"true".getBytes());
                    hasOrderedTable.put(put);
                    return true;
                }
            }
            return false;
        }
    }
}


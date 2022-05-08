package com.ms.rihuo;

import com.alibaba.fastjson.JSONObject;
import com.ms.dianshang.bean.LoginCommonLog;
import com.ms.dianshang.bean.LoginLog;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import scala.Tuple2;

import java.util.Date;
import java.util.Properties;

public class RihuoMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","192.168.126.128:9092");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,"wxwmd");

        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer<String>("gmall_start_0523", new SimpleStringSchema(), kafkaProps));
        SingleOutputStreamOperator<LoginLog> loginLogStream = kafkaSource.map(str -> JSONObject.parseObject(str, LoginLog.class));

        SingleOutputStreamOperator<Tuple2<String, Integer>> uv = loginLogStream
                .keyBy(new DateKeySelector())
                .process(new RihuoProcessFunction());

        uv.print();
        env.execute("first flink job");
    }

    /**
     * keySelector
     * 因为要做日活，所以要将日期作为key
     */
    static class DateKeySelector implements KeySelector<LoginLog, String>{
        @Override
        public String getKey(LoginLog loginLog) throws Exception {
            Date date = new Date(Long.parseLong(loginLog.getTs()));
            return date.getYear()+1900+"-"+date.getMonth()+"-"+date.getDate();
        }
    }

    /**
     * 统计日活，pass掉已经登录过的日志
     */
    static class RihuoProcessFunction extends ProcessFunction<LoginLog, Tuple2<String,Integer>> {
        private MapState<String,Boolean> hasLogin;
        private ValueState<Integer> count;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<String,Boolean> mapStateDescriptor=new MapStateDescriptor<String, Boolean>("has login?",String.class,Boolean.class);
            hasLogin = getRuntimeContext().getMapState(mapStateDescriptor);
            ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("uv", Integer.class);
            count=getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void processElement(LoginLog loginLog, ProcessFunction<LoginLog, Tuple2<String,Integer>>.Context context, Collector<Tuple2<String,Integer>> collector) throws Exception {
            String uid = loginLog.getCommon().getUid();
            // 如果还没登陆过
            if (!hasLogin.contains(uid)){
                hasLogin.put(uid,true);
                if (count.value()==null){
                    count.update(1);
                } else {
                    count.update(count.value()+1);
                }

                Date date = new Date(Long.parseLong(loginLog.getTs()));
                // 这个month是从0到11的，我也懒得管它了
                String logDate = date.getYear()+1900+"-"+date.getMonth()+"-"+date.getDate();
                collector.collect(Tuple2.apply(logDate,count.value()));
            }
        }
    }
}

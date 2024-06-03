package org.simple.flink.connectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

public class ConnectorsDemoRedis {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.Source
        DataStream<String> linesDS = env.socketTextStream("127.0.0.1", 9999);

        //3.Transformation
        //3.1切割并记为1
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = linesDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        //3.2分组
//        KeyedStream<Tuple2<String, Integer>, Tuple> groupedDS = wordAndOneDS.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOneDS.keyBy(tuple -> tuple.f0);
        //3.3聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = groupedDS.sum(1);

        //4.Sink
        result.print();
        // * 最后将结果保存到Redis
        // * 注意:存储到Redis的数据结构:使用hash也就是map
        // * key            value
        // * WordCount      (单词,数量)

        //-1.创建RedisSink之前需要创建RedisConfig
        //连接单机版Redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.43.161").build();
        //连接集群版Redis
        //HashSet<InetSocketAddress> nodes = new HashSet<>(Arrays.asList(new InetSocketAddress(InetAddress.getByName("node1"), 6379),new InetSocketAddress(InetAddress.getByName("node2"), 6379),new InetSocketAddress(InetAddress.getByName("node3"), 6379)));
        //FlinkJedisClusterConfig conf2 = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build();
        //连接哨兵版Redis
        //Set<String> sentinels = new HashSet<>(Arrays.asList("node1:26379", "node2:26379", "node3:26379"));
        //FlinkJedisSentinelConfig conf3 = new FlinkJedisSentinelConfig.Builder().setMasterName("mymaster").setSentinels(sentinels).build();

        //-3.创建并使用RedisSink
        result.addSink(new RedisSink<>(conf, new RedisWordCountMapper()));

        //5.execute
        env.execute();
    }

    /**
     * -2.定义一个Mapper用来指定存储到Redis中的数据结构
     */
    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "WordCount");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }
}

package org.simple.flink.other;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Broadcast {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.Source
        //学生数据集(学号,姓名)
        DataStream<User> studentDS = env.fromData(
                Arrays.asList(
                        new User(1, "张三"),
                        new User(2, "李四"),
                        new User(3, "王五"))
        );
        studentDS.print("student:");
        //成绩数据集(学号,学科,成绩)
        KeyedStream<Score, Integer> scoreIntegerKeyedStream = env.fromData(
                Arrays.asList(
                        new Score(1, "语文", 50),
                        new Score(2, "数学", 70),
                        new Score(3, "英文", 86))
        ).keyBy(score -> score.id);

        scoreIntegerKeyedStream.print("score:");
        //3.Transformation
        //将studentDS(学号,姓名)集合广播出去(广播到各个TaskManager内存中)
        //然后使用scoreDS(学号,学科,成绩)和广播数据(学号,姓名)进行关联,得到这样格式的数据:(姓名,学科,成绩)
        MapStateDescriptor<Integer, User> descriptor = new MapStateDescriptor<>("studentInfo",
                BasicTypeInfo.INT_TYPE_INFO,
                TypeInformation.of(new TypeHint<User>() {
                }));

        BroadcastStream<User> broadcast = studentDS.broadcast(descriptor);
        BroadcastConnectedStream<Score, User> connect = scoreIntegerKeyedStream.connect(broadcast);


        connect.process(new KeyedBroadcastProcessFunction<Integer, Score, User, Tuple4<Integer, String, String, Integer>>() {
            private final Map<Integer, List<Score>> cache = new ConcurrentHashMap<>();

            @Override
            public void processElement(Score value, KeyedBroadcastProcessFunction<Integer, Score, User, Tuple4<Integer, String, String, Integer>>.ReadOnlyContext ctx, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
                ReadOnlyBroadcastState<Integer, User> broadcastState = ctx.getBroadcastState(descriptor);
                User user = broadcastState.get(value.id);
                if (user != null) {
                    Tuple4<Integer, String, String, Integer> tuple4 = new Tuple4<>(
                            user.id,
                            user.name,
                            value.subject,
                            value.score
                    );
                    out.collect(tuple4);
                } else {
                    List<Score> scores = cache.computeIfAbsent(value.getId(), k -> new LinkedList<>());
                    scores.add(value);
                }
            }

            @Override
            public void processBroadcastElement(User value, KeyedBroadcastProcessFunction<Integer, Score, User, Tuple4<Integer, String, String, Integer>>.Context ctx, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
                BroadcastState<Integer, User> broadcastState = ctx.getBroadcastState(descriptor);
                broadcastState.put(value.getId(), value);
                if (cache.containsKey(value.getId())) {
                    List<Score> scores = cache.get(value.getId());
                    for (Score score : scores) {
                        Tuple4<Integer, String, String, Integer> tuple4 = new Tuple4<>(
                                score.id,
                                value.name,
                                score.subject,
                                score.score
                        );
                        out.collect(tuple4);
                    }
                }
            }

        }).print("broadcast:");

        //4.Sink
        env.execute("broadcast:");
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    static class User {
        private Integer id;
        private String name;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    static class Score {
        private Integer id;
        private String subject;
        private Integer score;
    }
}
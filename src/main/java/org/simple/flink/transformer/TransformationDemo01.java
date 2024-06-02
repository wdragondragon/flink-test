package org.simple.flink.transformer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformationDemo01 {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.source
        DataStream<String> linesDS = env.fromData("ylw hadoop spark", "ylw hadoop spark", "ylw hadoop", "ylw");


        //3.处理数据-transformation
        DataStream<String> wordsDS = linesDS.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            //value就是一行行的数据
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);//将切割处理的一个个的单词收集起来并返回
            }
        });
        DataStream<String> filtedDS = wordsDS.filter((FilterFunction<String>) value -> !value.equals("ylw"));
        DataStream<Tuple2<String, Integer>> wordAndOnesDS = filtedDS.map((MapFunction<String, Tuple2<String, Integer>>) value -> {
            //value就是进来一个个的单词
            return Tuple2.of(value, 1);
        });
        //KeyedStream<Tuple2<String, Integer>, Tuple> groupedDS = wordAndOnesDS.keyBy(0);
        KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOnesDS.keyBy(t -> t.f0);

        DataStream<Tuple2<String, Integer>> result1 = groupedDS.sum(1);
        DataStream<Tuple2<String, Integer>> result2 = groupedDS.reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1));

        //4.输出结果-sink
        result1.print("result1");
        result2.print("result2");

        //5.触发执行-execute
        env.execute();
    }
}
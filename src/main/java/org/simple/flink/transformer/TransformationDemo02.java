package org.simple.flink.transformer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class TransformationDemo02 {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.Source
        DataStream<String> ds1 = env.fromData("hadoop", "spark", "flink");
        DataStream<String> ds2 = env.fromData("hadoop", "spark", "flink");
        DataStream<Long> ds3 = env.fromData(1L, 2L, 3L);

        //3.Transformation
        DataStream<String> result1 = ds1.union(ds2);//合并但不去重 https://blog.csdn.net/valada/article/details/104367378
        ConnectedStreams<String, Long> tempResult = ds1.connect(ds3);
        //interface CoMapFunction<IN1, IN2, OUT>
        DataStream<String> result2 = tempResult.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String value) {
                return "String->String:" + value;
            }

            @Override
            public String map2(Long value) {
                return "Long->String:" + value.toString();
            }
        });

        //4.Sink
        result1.print("1");
        result2.print("2");

        //5.execute
        env.execute();
    }
}
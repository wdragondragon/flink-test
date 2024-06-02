package org.simple.flink.transformer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TransformationDemo03 {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2.Source
        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //3.Transformation
        /*SplitStream<Integer> splitResult = ds.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                //value是进来的数字
                if (value % 2 == 0) {
                    //偶数
                    ArrayList<String> list = new ArrayList<>();
                    list.add("偶数");
                    return list;
                } else {
                    //奇数
                    ArrayList<String> list = new ArrayList<>();
                    list.add("奇数");
                    return list;
                }
            }
        });
        DataStream<Integer> evenResult = splitResult.select("偶数");
        DataStream<Integer> oddResult = splitResult.select("奇数");*/

        //定义两个输出标签
        OutputTag<Integer> tag_even = new OutputTag<Integer>("偶数", TypeInformation.of(Integer.class));
        OutputTag<Integer> tag_odd = new OutputTag<Integer>("奇数") {
        };
        //对ds中的数据进行处理
        SingleOutputStreamOperator<Integer> tagResult = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                if (value % 2 == 0) {
                    //偶数
                    ctx.output(tag_even, value);
                } else {
                    //奇数
                    ctx.output(tag_odd, value);
                }
            }
        });

        //取出标记好的数据
        DataStream<Integer> evenResult = tagResult.getSideOutput(tag_even);
        DataStream<Integer> oddResult = tagResult.getSideOutput(tag_odd);

        //4.Sink
        evenResult.print("偶数");
        oddResult.print("奇数");

        //5.execute
        env.execute();
    }
}
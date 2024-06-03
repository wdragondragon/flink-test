package org.simple.flink.other;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class Accumulator {

    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 每20秒作为checkpoint的一个周期
//        env.enableCheckpointing(20000);
//        // 两次checkpoint间隔最少是10秒
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
//        // 程序取消或者停止时不删除checkpoint
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // checkpoint必须在60秒结束,否则将丢弃
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        // 同一时间只能有一个checkpoint
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 设置EXACTLY_ONCE语义,默认就是这个
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // checkpoint存储位置
//        env.getCheckpointConfig().setCheckpointStorage("file:///C:/dev/ideaProject/flink-quickstart-java/output/fileSink/checkpoint");
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //2.Source
        DataStream<String> dataDS = env.fromData("aaa", "bbb", "ccc", "ddd","ew2","aaa", "bbb", "ccc", "ddd","ew2");

        //3.Transformation
        SingleOutputStreamOperator<String> result = dataDS.map(new RichMapFunction<String, String>() {
            //-1.创建累加器
            private final IntCounter elementCounter = new IntCounter();
            Integer count = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //-2注册累加器
                getRuntimeContext().addAccumulator("elementCounter", elementCounter);
            }

            @Override
            public String map(String value) throws Exception {
                //-3.使用累加器
                this.elementCounter.add(1);
                count += 1;
                System.out.println("不使用累加器统计的结果:" + count);
                return value;
            }
        }).setParallelism(2);

        // 构造FileSink对象,这里使用的RowFormat,即行处理类型的
        FileSink<String> fileSink = FileSink
                // 配置文件输出路径及编码格式
                .forRowFormat(new Path("output/fileSink/"), new SimpleStringEncoder<String>("UTF-8"))
                // 设置文件滚动策略(文件切换)
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(180)) // 设置间隔时长180秒进行文件切换
                                .withInactivityInterval(Duration.ofMinutes(20)) // 文件20秒没有数据写入进行文件切换
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024)) // 设置文件大小1MB进行文件切换
                                .build()
                )
                // 分桶策略(划分子文件夹)
                .withBucketAssigner(new DateTimeBucketAssigner<>()) // 按照yyyy-mm-dd--h进行分桶
                //设置分桶检查时间间隔为100毫秒
                .withBucketCheckInterval(100)
                // 输出文件文件名相关配置
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("test") // 文件前缀
                                .withPartSuffix(".txt") // 文件后缀
                                .build()
                )
                .build();

        result.print();
        result.sinkTo(fileSink);
//        result.writeAsText("output/fileSink", FileSystem.WriteMode.OVERWRITE);
        //5.execute
        //-4.获取加强结果
        JobExecutionResult jobResult = env.execute();
        int nums = jobResult.getAccumulatorResult("elementCounter");
        System.out.println("使用累加器统计的结果:" + nums);
    }
}
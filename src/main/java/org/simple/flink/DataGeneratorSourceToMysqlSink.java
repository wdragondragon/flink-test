package org.simple.flink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.Random;
import java.util.UUID;

/**
 * 自定义Source
 *
 */
public class DataGeneratorSourceToMysqlSink {


    public static void main(String[] args) throws Exception {

        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        Random random = new Random();
        DataGeneratorSource<Order> dataGeneratorSource =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, Order>) value -> {
                            String id = UUID.randomUUID().toString().replace("-","").toUpperCase();
                            int userId = random.nextInt(1000);
                            int money = random.nextInt(1000);
                            long createTime = System.currentTimeMillis();
                            return new Order(id, userId, money, createTime);
                        },
                        Long.MAX_VALUE, //生成数据的最大值
                        RateLimiterStrategy.perSecond(10),  // 每秒钟生成多少条数据
                        Types.POJO(Order.class)    // 返回的数据类型
                );
        //2.Source
        DataStream<Order> orderDS = env
                .fromSource(dataGeneratorSource,WatermarkStrategy.noWatermarks(),"order-source")
                .setParallelism(2);

        orderDS.addSink(new MySQLSink());

        //3.Transformation

        //4.Sink
        orderDS.print();
        //5.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }
    public static class MySQLSink extends RichSinkFunction<Order> {
        private Connection conn = null;
        private PreparedStatement ps = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //加载驱动,开启连接
            //Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3305/test", "root", "951753");
            String sql = "INSERT INTO `flink_order` (`id`, `userId`, `money`,`createTime`) VALUES (?, ?, ?,?)";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void invoke(Order value, Context context) throws Exception {
            //给ps中的?设置具体值
            ps.setString(1, value.getId());
            ps.setInt(2, value.getUserId());
            ps.setInt(3, value.getMoney());
            ps.setTimestamp(4, new Timestamp(value.getCreateTime()));
            //执行sql
            ps.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
        }
    }
}

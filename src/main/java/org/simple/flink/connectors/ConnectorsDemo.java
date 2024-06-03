package org.simple.flink.connectors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConnectorsDemo {

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.Source
        env.fromData(new Student(null, "tonyma111", 18))
                //3.Transformation
                //4.Sink
                .addSink(JdbcSink.sink(
                        "INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (null, ?, ?)",
                        (ps, s) -> {
                            ps.setString(1, s.getName());
                            ps.setInt(2, s.getAge());
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://vmhost:3306/flink_test")
                                .withUsername("root")
                                .withPassword("951753")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .build()));
        //5.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }
}
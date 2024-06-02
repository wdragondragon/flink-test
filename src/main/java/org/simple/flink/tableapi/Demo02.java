package org.simple.flink.tableapi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Demo02 {

    public static void main(String[] args) throws Exception {
// create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// create a DataStream
        DataStream<WC> dataStream = env.fromData(
                new WC("Alice", 12),
                new WC("Bob", 10),
                new WC("Alice", 100));

// interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream);

// register the Table object as a view and query it
// the query contains an aggregation that produces updates
        tableEnv.createTemporaryView("InputTable", inputTable);
        Table resultTable = tableEnv.sqlQuery(
                "SELECT name, SUM(score) as score FROM InputTable GROUP BY name");

// interpret the updating Table as a changelog DataStream
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

// add a printing sink and execute in DataStream API
        resultStream.print();
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        public String name;
        public long score;
    }
}
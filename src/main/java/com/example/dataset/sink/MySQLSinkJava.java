package com.example.dataset.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class MySQLSinkJava {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env =
        ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Row> dataSource = env.fromElements(
        "spark hbase java",
        "java spark hive",
        "java hbase hbase")
        .flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
          for (String word : line.split(" ")) {
            collector.collect(new Tuple2<>(word, 1));
          }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
        .groupBy("f0")
        .reduce((x, y) -> (new Tuple2<>(x.f0, x.f1 + y.f1)))
        .map(x -> Row.of(x.f0, x.f1));

    dataSource.output(
        JDBCOutputFormat.buildJDBCOutputFormat()
            .setDrivername("com.mysql.jdbc.Driver")
            .setDBUrl("jdbc:mysql://bigdata-pro-m03.kfk.com/flink")
            .setUsername("root")
            .setPassword("123456")
            .setQuery("insert into wordcount (word, count) values (?,?)")
            .finish()
    );

    env.execute();

  }
}

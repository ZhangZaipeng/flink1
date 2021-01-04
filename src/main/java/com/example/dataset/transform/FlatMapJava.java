package com.example.dataset.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlatMapJava {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(2);

    DataSet<String> dataSource = env.fromElements(
        "spark hbase java",
        "java spark hive",
        "java hbase hbase"
    );

    DataSet<String> mapSource = dataSource.map(new MapFunction<String, String>() {
      @Override
      public String map(String line) throws Exception {
        return line.toUpperCase();
      }
    });

    DataSet<Tuple2<String, Integer>> result = mapSource.flatMap((String line,
        Collector<Tuple2<String, Integer>> collector) -> {
      for (String word : line.split(" ")) {
        collector.collect(new Tuple2<String, Integer>(word, 1));
      }
    }).returns(Types.TUPLE(Types.STRING, Types.INT));

    result.print();

  }
}

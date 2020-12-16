package com.example.dataset.transform;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CombineGroupJava {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env =
        ExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    DataSet<String> dataSource = env.fromElements(
        "spark hbase java",
        "java spark hive",
        "java hbase hbase"
    );

    DataSet<Tuple2<String, Integer>> result = dataSource.flatMap((String line,
        Collector<Tuple2<String, Integer>> collector) -> {
      for (String word : line.split(" ")) {
        collector.collect(new Tuple2<String, Integer>(word, 1));
      }
    }).returns(Types.TUPLE(Types.STRING, Types.INT))
        .groupBy("f0")
        .combineGroup((Iterable<Tuple2<String, Integer>> iterable,
            Collector<Tuple2<String, Integer>> collector) -> {
          String key = null;
          int count = 0;
          for (Tuple2<String, Integer> tuple2 : iterable) {
            key = tuple2.f0;
            count = count + tuple2.f1;
          }
          collector.collect(new Tuple2<>(key, count));
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
        .groupBy("f0")
        .reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
          @Override
          public void reduce(Iterable<Tuple2<String, Integer>> iterable,
              Collector<Tuple2<String, Integer>> collector) throws Exception {

            String key = null;
            int count = 0;
            for (Tuple2<String, Integer> tuple2 : iterable) {
              key = tuple2.f0;
              count = count + tuple2.f1;
            }

            collector.collect(new Tuple2<>(key, count));

          }
        });

    result.print();

//        env.execute() ;

  }
}

package com.example.dataset.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TextSourceJava {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env =
        ExecutionEnvironment.getExecutionEnvironment();
    DataSource<String> source = env.readTextFile(
        "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/wordcount");

    DataSet<Tuple2<String, Integer>> dataSet =
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
          @Override
          public void flatMap(String s, Collector<Tuple2<String, Integer>> collector)
              throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
              collector.collect(new Tuple2<String, Integer>(word, 1));
            }

          }
        })
            .groupBy("f0")
            .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
              @Override
              public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1,
                  Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1);
              }
            });

    dataSet.print();

  }
}

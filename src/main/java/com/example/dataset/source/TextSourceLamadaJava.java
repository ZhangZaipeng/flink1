package com.example.dataset.source;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TextSourceLamadaJava {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env =
        ExecutionEnvironment.getExecutionEnvironment();
    DataSource<String> source = env.readTextFile(
        "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/wordcount");

    DataSet<Tuple2<String, Integer>> dataSet =
        source.flatMap((String line, Collector<Tuple2<String, Integer>> collector) ->
        {
          String[] words = line.split(" ");
          for (String word : words) {
            collector.collect(new Tuple2<String, Integer>(word, 1));
          }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
            .groupBy("f0")
            .reduce((t1, t2) -> new Tuple2<String, Integer>(t1.f0, t1.f1 + t2.f1));
    dataSet.print();

  }
}

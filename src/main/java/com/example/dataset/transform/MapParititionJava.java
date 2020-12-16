package com.example.dataset.transform;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapParititionJava {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env =
        ExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(2);
    DataSet<String> dataSource = env.fromElements(
        "spark hbase java",
        "java spark hive",
        "java hbase hbase"
    )
//        .mapPartition(new MapPartitionFunction<String, String>() {
//            @Override
//            public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
//                for (String line : iterable){
//                    collector.collect(line.toUpperCase());
//                }
//            }
//        });
//
        .mapPartition((Iterable<String> iterable, Collector<String> collector) -> {
          for (String line : iterable) {
            collector.collect(line.toUpperCase());
          }
        }).returns(Types.STRING);

    dataSource.print();
  }
}

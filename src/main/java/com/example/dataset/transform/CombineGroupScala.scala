package com.example.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

object CombineGroupScala {
  def main(args: Array[String]): Unit = {
    val evn = ExecutionEnvironment.getExecutionEnvironment;
    val dataSource = evn.fromElements(
      "spark hbase java",
      "java spark hive",
      "java hbase hbase")
      .flatMap((line: String, collecotr: Collector[(String, Int)]) => {
        line.split(" ").foreach(word => collecotr.collect(word, 1))
      })
      .groupBy("_1")
      .combineGroup((iterator, combine_collector: Collector[(String, Int)]) => {
        combine_collector.collect(iterator reduce ((t1, t2) => (t1._1, t1._2 + t2._2)))
      })
      .groupBy("_1")
      .reduceGroup((x => x reduce ((x, y) => (x._1, x._2 + y._2))))

    dataSource.print()
  }
}

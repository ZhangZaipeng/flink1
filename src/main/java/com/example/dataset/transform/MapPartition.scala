package com.example.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

object MapPartition {


  def main(args: Array[String]): Unit = {
    val evn = ExecutionEnvironment.getExecutionEnvironment ;
    val dataSource = evn.fromElements(
      "spark hbase java",
      "java spark hive",
      "java hbase hbase"
    )
//      .mapPartition((iterator : Iterator[String] , collector : Collector[String]) => {
//           for (line <- iterator) {
//             collector.collect(line.toUpperCase)
//           }
//      })
      .print()


  }
}

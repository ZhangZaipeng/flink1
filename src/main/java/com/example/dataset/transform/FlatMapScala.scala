package com.example.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

object FlatMapScala {
  def main(args: Array[String]): Unit = {
    val evn = ExecutionEnvironment.getExecutionEnvironment;
    val dataSource = evn.fromElements(
      "spark hbase java",
      "java spark hive",
      "java hbase hbase"
    )
      .map(line => line.toUpperCase)
      .flatMap((line: String, collecotr: Collector[(String, Int)]) => {

        (line.split(" ")).foreach(word => collecotr.collect(word, 1))
        //              for(word <- line.split(" ")){
        //                collecotr.collect(word ,1 )
        //              }
      })
      //      .flatMap(line => line.split(" "))
      //      .map(word => (word , 1))
      .print()

  }
}

package com.example.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

object ReduceGroupScala {
  def main(args: Array[String]): Unit = {
    val evn = ExecutionEnvironment.getExecutionEnvironment;
    val dataSource = evn.fromElements(
      "spark hbase java",
      "java spark hive",
      "java hbase hbase"
    )
      .flatMap((line : String , collecotr :Collector[(String,Int)]) =>{

        (line.split(" ")).foreach(word => collecotr.collect(word , 1))
      })
      .groupBy("_1")
        .reduceGroup((x => x reduce((x , y) => (x._1 , x._2 + y._2))))
      .print()
  }
}

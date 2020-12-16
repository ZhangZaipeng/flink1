package com.example.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment,_}

object MapScala {

  def main(args: Array[String]): Unit = {

    val evn = ExecutionEnvironment.getExecutionEnvironment ;
    val dataSource = evn.fromElements(
      "spark hbase java",
      "java spark hive",
      "java hbase hbase"
    )
      .map(line => line.toUpperCase)
        .print()
  }
}

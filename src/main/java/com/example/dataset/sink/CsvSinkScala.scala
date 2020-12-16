package com.example.dataset.sink

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object CsvSinkScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(1);
    val dataSource = env.fromElements(
      "spark hbase java",
      "java spark hive",
      "java hbase hbase"
    )
      .flatMap((line : String , collecotr :Collector[(String,Int)]) =>{

        (line.split(" ")).foreach(word => collecotr.collect(word , 1))
      })

      .groupBy("_1")
        .reduceGroup(x => x reduce((x , y) => (x._1,x._2 + y._2)))
      .writeAsCsv("/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources//csvSink.csv",
        "\n"," ",
        WriteMode.NO_OVERWRITE);


    env.execute() ;


  }

}

package com.example

import org.apache.flink.streaming.api.scala._

object NetworkWordCount {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text: DataStream[String] = env.socketTextStream("192.172.1.40", 9999)

    val counts = text.flatMap(_.toLowerCase().split(" "))
      .map((_, 1)).keyBy(0).sum(1)

    counts.print()

    env.execute("Streaming Count")
  }
}

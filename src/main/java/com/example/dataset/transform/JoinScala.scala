package com.example.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector

object JoinScala {

  case class Employee(dept_id: String, emp_name: String, emp_price: Int)

  def main(args: Array[String]): Unit = {
    val evn = ExecutionEnvironment.getExecutionEnvironment;

    val dept_source = evn.readCsvFile[(String, String)](
      "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/dept.csv");

    val employee_source = evn.readCsvFile[Employee](
      "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/employee.csv");

    //    dept_source.join(employee_source).where("_1").equalTo("dept_id")
    //        .map(tupe2 => (tupe2._2.emp_name,tupe2._1._2,tupe2._2.emp_price))
    dept_source.join(employee_source).where("_1").equalTo("dept_id") {
      (x, y) => (y.emp_name, x._2, y.emp_price)
    }
      .print()

  }

}

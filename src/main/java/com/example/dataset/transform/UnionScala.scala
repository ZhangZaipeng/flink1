package com.example.dataset.transform

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object UnionScala {

  case class employee(dept_id: String, emp_name: String, emp_price: Int)

  def main(args: Array[String]): Unit = {
    val evn = ExecutionEnvironment.getExecutionEnvironment;

    val dept = evn.readCsvFile[(String, String)](
      "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/dept.csv");

    val employee_1 = evn.readCsvFile[employee](
      "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/employee.csv");

    val employee_2 = evn.readCsvFile[employee](
      "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/employee_2.csv");

    employee_1.union(employee_2)
      .map(emp => (emp.dept_id, emp.emp_price))
      .groupBy("_1")
      .reduce((x, y) => (x._1, x._2 + y._2))
      .join(dept).where(0).equalTo(0)
      .map(x => (x._1._1, x._2._2, x._1._2))

      .print()

    //      .groupBy("dept_id")
    //      .reduce((emp_1 ,emp_2) -> {
    //           emp_1.
    //      })

  }

}

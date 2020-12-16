package com.example.dataset.transform;

import com.example.base.EmploeePOJO;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class JoinJava {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env =
        ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Tuple2<String, String>> dept_source = env.readCsvFile(
        "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/dept.csv")
        .types(String.class, String.class);

    DataSet<EmploeePOJO> employee_source = env.readCsvFile(
        "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/employee.csv")
        .pojoType(EmploeePOJO.class, "dept_id", "emp_name", "emp_price");

    DataSet<Tuple3<String, String, Integer>> result =
        dept_source.join(employee_source).where("f0").equalTo("dept_id")
            .map((Tuple2<Tuple2<String, String>, EmploeePOJO> tuple) -> {
              return new Tuple3<>(tuple.f1.emp_name, tuple.f0.f1, tuple.f1.emp_price);
            }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));

    result.print();
  }
}

package com.example.dataset.transform;

import com.example.base.DeptPOJO;
import com.example.base.DeptPricePOJO;
import com.example.base.EmploeePOJO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class UnionJava {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env =
        ExecutionEnvironment.getExecutionEnvironment();
    DataSet<DeptPOJO> dept_source = env.readCsvFile(
        "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/dept.csv")
        .pojoType(DeptPOJO.class, "dept_id", "dept_name");

    DataSet<EmploeePOJO> employee_1 = env.readCsvFile(
        "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/employee.csv")
        .pojoType(EmploeePOJO.class, "dept_id", "emp_name", "emp_price");

    DataSet<EmploeePOJO> employee_2 = env.readCsvFile(
        "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/employee_2.csv")
        .pojoType(EmploeePOJO.class, "dept_id", "emp_name", "emp_price");

    DataSet<DeptPricePOJO> result = employee_1.union(employee_2)
        .map(new MapFunction<EmploeePOJO, DeptPricePOJO>() {
          @Override
          public DeptPricePOJO map(EmploeePOJO emploeePOJO) throws Exception {
            return new DeptPricePOJO(emploeePOJO.dept_id, emploeePOJO.emp_price);
          }
        })
        .groupBy("dept_id")
        .reduce((x, y) -> {
          return new DeptPricePOJO(x.dept_id, x.dept_price + y.dept_price);
        });

    DataSet<Tuple3<String, String, Integer>> dept_result = result
        .join(dept_source).where("dept_id").equalTo("dept_id")
        .map(t1 -> {
          return new Tuple3<String, String, Integer>(t1.f1.dept_id,
              t1.f1.dept_name, t1.f0.dept_price);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))

        .first(2);

    dept_result.print();

//        result.print();
  }
}

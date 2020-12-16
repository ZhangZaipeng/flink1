package com.example.base;

public class EmploeePOJO {

  public String dept_id;
  public String emp_name;
  public int emp_price;

  public EmploeePOJO() {
  }

  public EmploeePOJO(String dept_id, String emp_name, int emp_price) {
    this.dept_id = dept_id;
    this.emp_name = emp_name;
    this.emp_price = emp_price;
  }

  @Override
  public String toString() {
    return "EmploeePOJO{" +
        "dept_id='" + dept_id + '\'' +
        ", emp_name='" + emp_name + '\'' +
        ", emp_price=" + emp_price +
        '}';
  }
}

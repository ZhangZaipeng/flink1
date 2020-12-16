package com.example.base;

public class DeptPOJO {

  public String dept_id;
  public String dept_name;

  public DeptPOJO() {
  }

  public DeptPOJO(String dept_id, String dept_name) {
    this.dept_id = dept_id;
    this.dept_name = dept_name;
  }

  @Override
  public String toString() {
    return "DeptPOJO{" +
        "dept_id='" + dept_id + '\'' +
        ", dept_name='" + dept_name + '\'' +
        '}';
  }
}

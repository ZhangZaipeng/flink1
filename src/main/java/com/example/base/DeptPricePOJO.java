package com.example.base;

public class DeptPricePOJO {

  public String dept_id;
  public int dept_price;

  public DeptPricePOJO(String dept_id, int dept_price) {
    this.dept_id = dept_id;
    this.dept_price = dept_price;
  }

  public DeptPricePOJO() {

  }

  @Override
  public String toString() {
    return "DeptProciePOJO{" +
        "dept_id='" + dept_id + '\'' +
        ", dept_price=" + dept_price +
        '}';
  }
}

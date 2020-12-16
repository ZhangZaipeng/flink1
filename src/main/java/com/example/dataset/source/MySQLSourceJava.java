package com.example.dataset.source;

import com.example.base.WordCountPOJO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class MySQLSourceJava {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env =
        ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Row> dbData =
        env.createInput(
            JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://bigdata-pro-m03.kfk.com/flink")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("select * from wordcount")
                .setRowTypeInfo(
                    new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                .finish()
        );

    DataSet<WordCountPOJO> result = dbData.map(new MapFunction<Row, WordCountPOJO>() {
      @Override
      public WordCountPOJO map(Row row) throws Exception {
        return new WordCountPOJO(String.valueOf(row.getField(0)),
            (Integer) row.getField(1));
      }
    })
        .groupBy("word")
        .reduce(new ReduceFunction<WordCountPOJO>() {
          @Override
          public WordCountPOJO reduce(WordCountPOJO t1, WordCountPOJO t2) throws Exception {
            return new WordCountPOJO(t1.word, t1.count + t2.count);
          }
        });

    result.print();
  }
}

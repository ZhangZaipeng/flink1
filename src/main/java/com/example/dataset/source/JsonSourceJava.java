package com.example.dataset.source;

import com.example.base.WordCountPOJO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSourceJava {

  final static ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<WordCountPOJO> dataSet = env.readTextFile("wordcount.json")
        .map(new MapFunction<String, WordCountPOJO>() {
          @Override
          public WordCountPOJO map(String line) throws Exception {
            WordCountPOJO wordCountPOJO = objectMapper.readValue(line, WordCountPOJO.class);
            return wordCountPOJO;
          }
        })
        .groupBy("word")
        .reduce(new ReduceFunction<WordCountPOJO>() {
          @Override
          public WordCountPOJO reduce(WordCountPOJO t1, WordCountPOJO t2) throws Exception {
            return new WordCountPOJO(t1.word, t1.count + t2.count);
          }
        });

    dataSet.print();


  }
}

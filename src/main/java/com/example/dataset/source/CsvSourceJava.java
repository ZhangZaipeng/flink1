package com.example.dataset.source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class CsvSourceJava {

    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env =
                ExecutionEnvironment.getExecutionEnvironment();

//        DataSet<WordCountPOJO> dataSet = env.readCsvFile(
//                "/Users/caojinbo/Documents/workspace/aikfk_flink/src/main/resources/wordcount.csv")
//                .pojoType(WordCountPOJO.class,"word","count")
//                .groupBy("word")
//                .reduce((x,y) -> new WordCountPOJO(x.word,x.count + y.count));

        DataSet<Tuple2<String,Integer>> dataSet = env.readCsvFile("wordcount.csv")
                .includeFields("110")
                .types(String.class,Integer.class)
                .groupBy("f0")
                .reduce((x, y) -> new Tuple2<String,Integer>(x.f0,x.f1 + y.f1));

        dataSet.print();
    }
}

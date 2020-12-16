package com.example.base;

public class WordCountPOJO {

  public String word;
  public int count;

  public WordCountPOJO(String word, int count) {
    this.word = word;
    this.count = count;
  }

  public WordCountPOJO() {
  }

  @Override
  public String toString() {
    return "WordCountPOJO{" +
        "word='" + word + '\'' +
        ", count=" + count +
        '}';
  }
}

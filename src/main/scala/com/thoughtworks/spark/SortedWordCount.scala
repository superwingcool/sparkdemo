package com.thoughtworks.spark

import org.apache.spark.{SparkConf, SparkContext}

object SortedWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCount")
    val sparkContext = SparkContext.getOrCreate(conf)

    val file = sparkContext.textFile("./data/word.txt")
    val wordCounts = file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    val countWords = wordCounts.map(wordCount => (wordCount._2, wordCount._1))
    val sortedCountWords = countWords.sortByKey(false)
    sortedCountWords.saveAsTextFile("./result/sourtedword")

    sparkContext.stop()
  }

}

package com.thoughtworks.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//    val sparkSession = SparkSession.builder()
//      .appName("WordCount")
//      .master("local[2]")
//      .getOrCreate()


object WordCount {

  def main(args: Array[String]): Unit = {

    if(args.size < 2) {
      println("Please usage inputFile, outputFile!")
      System.exit(-1)
    }

    val Array(inputFile, outputFile) = args

    val conf = new SparkConf()
//      .setMaster("local[*]")
      .setAppName("WordCount")
    val sparkContext = SparkContext.getOrCreate(conf)

    val file = sparkContext.textFile(inputFile)
    val wordCounts = file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    wordCounts.saveAsTextFile(outputFile)

    sparkContext.stop()
  }

}

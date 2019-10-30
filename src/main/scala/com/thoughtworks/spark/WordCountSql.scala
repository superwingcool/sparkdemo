package com.thoughtworks.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WordCountSql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    spark.read
      .text("./data/word.txt")
      .withColumn("word", explode(split(col("value"), " ")))
      .select("word")
      .groupBy("word")
      .count()
      .orderBy("word")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("quoteAll", false)
      .option("quote", " ")
      .text("./result/wordsql")

    spark.close()

  }
}

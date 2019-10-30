package com.thoughtworks.spark

import java.util

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AreaTop3ProductTask {

  val sparkSession = SparkSession.builder()
    .appName("AreaTop3ProductTask")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    try {

      // udf函数
      sparkSession.udf.register("concat_id_and_name", (cityId: Long, name: String) => cityId.toString.concat(":").concat(name))
      sparkSession.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF)

      // 1 获取user_visitor
      val userVisitorInfoDF = getUserVisitorInfoDF

      // 2 获取city
      val cityInfoDF = getCityInfoDF

      // 3 获取product
      val productDF = getProductDF

      import sparkSession.implicits._

      val userVisitorCityInfoDF = userVisitorInfoDF.select("click_product_id", "city_id")
        .join(cityInfoDF, "city_id")
        .groupBy($"area", $"click_product_id", expr("concat_id_and_name(city_id, city_name) as city_info"))
        .agg(count("click_product_id").as("click_product_count"))
        .groupBy($"area", $"click_product_id", expr("click_product_count"))
        .agg(expr("group_concat_distinct(city_info) as cities_info"))


      userVisitorCityInfoDF.join(productDF, $"product_id" === $"click_product_id")
        .select($"area", $"cities_info", $"product_id", $"click_product_count",
          row_number().over(Window.partitionBy(col("area"))
            .orderBy(col("click_product_count").desc)).as("rank")
        )
        .filter("rank <=3")
        .show(false)


    } finally {
      if (sparkSession != null) {
        sparkSession.close()
      }
    }

  }

  private def getProductDF(): DataFrame = {
    val schema = DataTypes.createStructType(util.Arrays.asList(
      DataTypes.createStructField("product_id", DataTypes.LongType, true),
      DataTypes.createStructField("product_name", DataTypes.StringType, true),
      DataTypes.createStructField("extend_info", DataTypes.StringType, true)
    ))
    sparkSession.read.schema(schema).csv("data/products.csv")
  }

  private def getCityInfoDF(): DataFrame = {
    sparkSession.read.option("header", true).csv("data/city.csv")
  }


  private def getUserVisitorInfoDF(): DataFrame = {

    val schema = DataTypes.createStructType(util.Arrays.asList(
      DataTypes.createStructField("date", DataTypes.DateType, true),
      DataTypes.createStructField("user_id", DataTypes.LongType, true),
      DataTypes.createStructField("session_id", DataTypes.StringType, true),
      DataTypes.createStructField("page_id", DataTypes.LongType, true),
      DataTypes.createStructField("action_time", DataTypes.StringType, true),
      DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
      DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
      DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
      DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
      DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
      DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
      DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
      DataTypes.createStructField("city_id", DataTypes.LongType, true)
    ))

    sparkSession.read.schema(schema).csv("data/user_visitor_city.csv")
      .filter("click_product_id != 0")
  }

}
package com.thoughtworks.spark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(Array(StructField("cityInfo", StringType)))

  override def bufferSchema: StructType = StructType(Array(StructField("bufferCityInfo", StringType)))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, "")

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityInfoBuffer = buffer.getString(0)
    val cityInfo = input.getString(0)
    var result = cityInfoBuffer
    if(!cityInfoBuffer.contains(cityInfo)) {
      result = cityInfoBuffer.isEmpty match {
        case true => cityInfoBuffer.concat(cityInfo)
        case false => cityInfoBuffer.concat(",").concat(cityInfo)
      }
    }

    buffer.update(0, result)

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val bufferCityInfo1 = buffer1.getString(0)
    val bufferCityInfo2 = buffer2.getString(0)
    var result = bufferCityInfo1
    val dd = bufferCityInfo2.split(",")
      .filter(!bufferCityInfo1.contains(_))
      .toList
      dd.foreach(d => {
        result = StringUtils.isNotEmpty(bufferCityInfo1) match {
          case true => bufferCityInfo1.concat(",").concat(d)
          case false => bufferCityInfo1.concat(d)
        }
      })

    buffer1.update(0, result)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}

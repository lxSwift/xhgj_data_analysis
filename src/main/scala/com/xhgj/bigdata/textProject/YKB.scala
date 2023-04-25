package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.JsonParse
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source

object YKB {
  Logger.getLogger("src").setLevel(Level.INFO)
  def main(args: Array[String]): Unit = {
    val schema = StructType(Array(
      StructField("titile", StringType, nullable = true),
      StructField("standard", StringType, nullable = true),
      StructField("standardUnit", StringType, nullable = true)
    ))

    val spark = SparkSession.builder().appName("Spark Hive Demo").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    //val userDF =spark.read.json("file:///D:/response.json")
    val lines: String = Source.fromFile("D:\\response2.json").mkString
    val res: RDD[(String, String, String)] = sc.parallelize(JsonParse.amount(lines))
    val resRDD = res.map(tup => Row(tup._1,tup._2,tup._3))
    val resDF: DataFrame = spark.createDataFrame(resRDD,schema)
    resDF.show()
    sc.stop()
    spark.stop()
  }
}

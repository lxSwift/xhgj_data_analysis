package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.{JsonParse, TableName}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.io.Source

object YKB_FeeType {
  def main(args: Array[String]): Unit = {
    val schema = StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("parentId", StringType, nullable = true),
      StructField("active", StringType, nullable = true),
      StructField("code", StringType, nullable = true)
    ))
    val spark = SparkSession.builder().appName("Spark Hive YKBDATA_FEETYPE").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    //val userDF =spark.read.json("file:///D:/response.json")

    val lines: String = Source.fromFile("/data/ykbData/ykbData_feetype.json").mkString
//    val lines = sc.textFile("hdfs://172.16.104.238:9000/user/ykbData/*").toString()
    val res = sc.parallelize(JsonParse.feeTypes(lines))
    val resRDD = res.map(tup => Row(tup._1,tup._2,tup._3,tup._4,tup._5))
    val resDF: DataFrame = spark.createDataFrame(resRDD,schema)
    run(resDF,spark)
    sc.stop()
    spark.stop()
  }

  def run(resDF: DataFrame,spark:SparkSession): Unit = {
    resDF.createOrReplaceTempView("FEETY")
    spark.sql(
      """
        |SELECT
        | *
        |FROM
        | FEETY
        |WHERE
        | active = 'true'
        |""".stripMargin).createOrReplaceTempView("FEETYPE_A")
    spark.sql(
      s"""
        |INSERT OVERWRITE TABLE ${TableName.FEE_TYPR_TABLENAEM}
        |SELECT
        | *
        |FROM
        | FEETYPE_A
        |""".stripMargin)
  }
}

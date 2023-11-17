package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.{Config, MysqlConnect, PathUtil, TableName}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ceshi {
  val takeRow = 20

  case class Params(inputDay: String =null)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job ceshi.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession)= {
    /**
     * 获取手工的2022-12-31日期以前的已开票应收款余额
     */
    val sc = spark.sparkContext
    PathUtil.deleteExistedPath(sc,"/data/file/instock")

    spark.sql(
      s"""
         |SELECT oes.fnumber,
         |  dun.fname c_unit
         | FROM ${TableName.DIM_MATERIAL} oes
         |LEFT JOIN ${TableName.ODS_ERP_MATERIALBASE_DA} OESE ON oes.FMATERIALID = OESE.FMATERIALID
         |left join ${TableName.DIM_UNIT} dun on OESE.FBASEUNITID = dun.funitid
         |""".stripMargin).createOrReplaceTempView("tmp1")

    MysqlConnect.getMysqlData("ads_oth_material",spark).createOrReplaceTempView("tmp2")

    spark.sql(
      s"""
         |SELECT
         |a.fnumber,
         |a.c_unit,
         |b.unname
         |FROM tmp1 a left join tmp2 b on a.fnumber = b.mcode
         | where a.c_unit != b.unname
         |""".stripMargin).coalesce(1).write.csv("/data/file/instock")


//      .coalesce(1).write.csv("/data/file/instock")


  }






}

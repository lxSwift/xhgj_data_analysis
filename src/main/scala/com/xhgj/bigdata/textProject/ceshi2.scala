package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/8/23 10:05
 * @PackageName:com.xhgj.bigdata.textProject
 * @ClassName: ceshi2
 * @Description: TODO
 * @Version 1.0
 */
object ceshi2 {
  val takeRow = 20

  case class Params(inputDay: String = null)

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
  def runRES(spark: SparkSession)={

    //FISOVERLEGALORG组织间结算跨法人标识 为1 代表是   0代表否
//    val result = spark.sql(
//      s"""
//         |SELECT *
//         |FROM ${TableName.ODS_ERP_STKINVINITDETAIL_DA} limit 10
//         |""".stripMargin)
//    println("res========="+result.count())
  val result = spark.sql(
      s"""
         |SELECT
         |  dp.fnumber
         |from ${TableName.DWD_SAL_ORDER} oes
         |left join ${TableName.DIM_PROJECTBASIC} dp on oes.F_PROJECTNO  = dp.fid
         |where oes.fbillno='XSDD2023448661'
         |""".stripMargin)
    println("res========="+result.count())

    result.show(100,false)


  }

}

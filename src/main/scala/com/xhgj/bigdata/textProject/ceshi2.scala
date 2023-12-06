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
val res = spark.sql(
  s"""
     |select cast (oes.fcreatedate as varchar(32)) as createdate,
     | cast(oes.fqty as decimal(19,4)) as qty,
     | cast(oes.f_paez_text as varchar(64)) as consignee,
     | cast(oes.f_paez_text1 as varchar(64))  as contactphone,
     | cast(oes.f_paez_text2 as varchar(500)) as shppingaddress
     |from ${TableName.DWD_SAL_ORDER} oes
     |where COALESCE(oes.FORDERTYPE,0) <> 1
     |""".stripMargin)
    println(res.count())


  }

}

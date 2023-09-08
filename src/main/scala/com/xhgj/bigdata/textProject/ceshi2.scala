package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.TableName
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
    spark.sql(
      s"""
         |SELECT COUNT(*)
         |FROM ${TableName.ODS_ERP_REQUISITION} OER
         |LEFT JOIN ${TableName.ODS_ERP_REQENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.ODS_ERP_REQENTRY_S} OERS ON OERE.FENTRYID = OERS.FENTRYID
         |LEFT JOIN ${TableName.DIM_BUYER} DB ON OERS.FPURCHASERID = DB.FID
         |WHERE (OER.FAPPLICATIONORGID = '1' or OER.FAPPLICATIONORGID = '481351') AND OER.FDOCUMENTSTATUS = 'C'
         |AND SUBSTRING(OER.FAPPROVEDATE,1,10) >= '2023-01-01' AND SUBSTRING(OER.FAPPROVEDATE,1,10) <= '2023-08-31'
         |""".stripMargin).show()


  }

}

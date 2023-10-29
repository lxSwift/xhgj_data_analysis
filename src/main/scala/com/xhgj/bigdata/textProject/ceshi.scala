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
//    spark.sql(
//      s"""
//         |SELECT
//         |  oei.FAPPROVEDATE c_approve_date, --审核日期
//         |  DS.FNAME c_supplier_name, --供应商
//         |   MAT.fnumber c_material_no, --物料编码
//         |   oeief.FAMOUNT_LC
//         |FROM ${TableName.ODS_ERP_INSTOCK} oei
//         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
//         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_F} oeief ON oeie.FENTRYID = oeief.FENTRYID
//         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oeie.FMATERIALID = MAT.FMATERIALID
//         |LEFT JOIN ${TableName.DIM_SUPPLIER} DS ON oei.FSUPPLIERID = DS.FSUPPLIERID
//         |WHERE oei.FDOCUMENTSTATUS = 'C' and oeie.FSTOCKFLAG='1' and oei.FSTOCKORGID = '1' and oei.FAPPROVEDATE >= '2021-01-01' and oei.FAPPROVEDATE <= '2022-12-31'
//         |""".stripMargin).coalesce(1).write.csv("/data/file/instock")


  }






}

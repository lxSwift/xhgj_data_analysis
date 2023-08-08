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


    spark.sql(
      """select
      OER.FBILLNO, --增值税发票号
      OER.FDATE, --业务日期
      B.fname CUSTOMERNAME,--客户名称
      B.fnumber CUSTOMERNUM,
      OER.FALLAMOUNTFOR, --价税合计
      OERE.FALLAMOUNTFOR FAMOUNTFOR,
      C.fname SETTLEORGNAME, --结算组织
      D.FNUMBER MATERIALID, --物料编码
      OERE.FNOTAXAMOUNTFOR, --不含税金额
      OERE.F_PAEZ_Text salnum, --销售单号
      OERE.F_PXDF_TEXT projectnum, --项目编码
      J.FNAME AS SALENAME
      FROM ods_xhgj.ODS_ERP_RECEIVABLE OER
      LEFT JOIN ods_xhgj.ODS_ERP_RECEIVABLEENTRY OERE ON OER.FID = OERE.FID
      LEFT JOIN dw_xhgj.DIM_CUSTOMER DC ON OER.FCUSTOMERID = DC.FCUSTID
      LEFT JOIN dw_xhgj.DIM_CUSTOMER B ON OER.FCUSTOMERID = B.fcustid
      left join dw_xhgj.DIM_Organizations C on oer.FSETTLEORGID = C.forgid
      JOIN dw_xhgj.DIM_MATERIAL D ON OERE.FMATERIALID = D.FMATERIALID
      LEFT JOIN dw_xhgj.DIM_SALEMAN J ON OERE.F_PAEZ_BASE2 = J.FID
      WHERE DC.F_PAEZ_CHECKBOX = 0 AND COALESCE(OER.F_ORDERTYPE,0) <> 1 AND OER.FDOCUMENTSTATUS = 'C' AND SUBSTRING(OER.FDATE,1,4) >= 2021
      """).coalesce(1).write.csv("/data/file/yingshoudan.csv")


  }
}

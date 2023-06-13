package com.xhgj.bigdata.micro

import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/6/12 17:27
 * @PackageName:com.xhgj.bigdata.micro
 * @ClassName: PayAmount_Status
 * @Description: TODO
 * @Version 1.0
 */
object PayAmount_Status {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job PayAmount_Status.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession)={
    //应收单相关信息
    spark.sql(
      s"""
         |
         |select
         |	pro.fname PRONAME,
         |	pro.fnumber PRONO,
         |	cus.Fname CUSTNAME,
         |	cus.fnumber fnumber,
         |	sal.fname salename,
         |	sal.fnumber fnumber,
         |	a.F_PXDF_TEXT43 FaPiaoNo,
         |	a.F_PXDF_DATE FaPiaoDate,
         |	a.FALLAMOUNTFOR ALLAMOUNTFOR
         |from
         |	ODS_XHGJ.ODS_ERP_RECEIVABLE a
         |join ODS_XHGJ.ODS_ERP_RECEIVABLEENTRY b on a.fid=b.fid
         |join DW_XHGJ.DIM_PROJECTBASIC pro on b.FPROJECTNO=pro.fid
         |left join DW_XHGJ.DIM_CUSTOMER cus on cus.fcustid=a.FCUSTOMERID
         |left join DW_XHGJ.DIM_SALEMAN sal on sal.fid=a.FSALEERID
         |where a.FDOCUMENTSTATUS='C'
         |""".stripMargin)

  }
}

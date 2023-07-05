package com.xhgj.bigdata.micro

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/7/4 17:29
 * @PackageName:com.xhgj.bigdata.micro
 * @ClassName: InvoicingStatus
 * @Description: 开票状态
 * @Version 1.0
 */
object InvoicingStatus {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job PayAmount_Ready.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession)={

    /**
     * 【判定开票状态】
     * FRECEAMOUNT--开票金额
     * FSURPLUSRECEAMOUNT--未开票金额
     * 全部开票：
     * 1.开票金额＞0&&未开票金额≤0
     * 2.开票金额＞0&&未开票金额≥0&&大票关闭状态已关闭；
     *
     * 部分开票：
     * 开票金额＞0&&未开票金额＞0&&大票关闭状态未关闭
     *
     * 未开票：
     * 开票金额≤0
     */
    val res= spark.sql(
      s"""
         |SELECT
         |  FBILLNO,--项目编号
         |  CASE
         |    WHEN FCLOSESTATUS = 'A' THEN '未关闭'
         |    WHEN FCLOSESTATUS = 'B' THEN '关闭'
         |    ELSE '未知' END AS CLOSESTATUS,--关闭状态
         |  CASE
         |    WHEN COALESCE(FRECEAMOUNT,0) > 0 AND COALESCE(FSURPLUSRECEAMOUNT,0)<=0 THEN '全部开票'
         |    WHEN COALESCE(FRECEAMOUNT,0) > 0 AND COALESCE(FSURPLUSRECEAMOUNT,0)>=0 AND FCLOSESTATUS='B' THEN '全部开票'
         |    WHEN COALESCE(FRECEAMOUNT,0) > 0 AND COALESCE(FSURPLUSRECEAMOUNT,0)>0 AND FCLOSESTATUS='A' THEN '部分开票'
         |    WHEN COALESCE(FRECEAMOUNT,0) <=0 THEN '未开票'
         |    ELSE '其他' END AS INVOICESTATUS,--开票状态
         |  FRECEAMOUNT,--开票金额
         |  FSURPLUSRECEAMOUNT --未开票金额
         |FROM
         |  ${TableName.ODS_ERP_BIGTICKETPROJECT}
         |""".stripMargin)

    val table = "ads_status_invoicing"

    MysqlConnect.overrideTable(table,res)

  }

}

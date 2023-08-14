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


//    spark.sql(
//      """select
//      OER.FBILLNO, --增值税发票号
//      OER.FDATE, --业务日期
//      B.fname CUSTOMERNAME,--客户名称
//      B.fnumber CUSTOMERNUM,
//      OER.FALLAMOUNTFOR, --价税合计
//      OERE.FALLAMOUNTFOR FAMOUNTFOR,
//      C.fname SETTLEORGNAME, --结算组织
//      D.FNUMBER MATERIALID, --物料编码
//      OERE.FNOTAXAMOUNTFOR, --不含税金额
//      OERE.F_PAEZ_Text salnum, --销售单号
//      OERE.F_PXDF_TEXT projectnum, --项目编码
//      J.FNAME AS SALENAME
//      FROM ods_xhgj.ODS_ERP_RECEIVABLE OER
//      LEFT JOIN ods_xhgj.ODS_ERP_RECEIVABLEENTRY OERE ON OER.FID = OERE.FID
//      LEFT JOIN dw_xhgj.DIM_CUSTOMER DC ON OER.FCUSTOMERID = DC.FCUSTID
//      LEFT JOIN dw_xhgj.DIM_CUSTOMER B ON OER.FCUSTOMERID = B.fcustid
//      left join dw_xhgj.DIM_Organizations C on oer.FSETTLEORGID = C.forgid
//      JOIN dw_xhgj.DIM_MATERIAL D ON OERE.FMATERIALID = D.FMATERIALID
//      LEFT JOIN dw_xhgj.DIM_SALEMAN J ON OERE.F_PAEZ_BASE2 = J.FID
//      WHERE DC.F_PAEZ_CHECKBOX = 0 AND COALESCE(OER.F_ORDERTYPE,0) <> 1 AND OER.FDOCUMENTSTATUS = 'C' AND SUBSTRING(OER.FDATE,1,4) >= 2021
//      """).coalesce(1).write.csv("/data/file/yingshoudan.csv")

    spark.sql(
      s"""
         |SELECT DS.FNAME AS SALENAME
         |	,SUBSTRING(OER.FDATE,1,10) AS BUSINESSDATE
         | ,OES.F_PAEZ_CHECKBOX
         |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR DP.fnumber LIKE '%HZXM%') THEN '非自营'
         |		ELSE '自营' END AS PERFORMANCEFORM
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END AS PROJECTSHORTNAME
         |	,CAST(SUM(OERE.FPRICEQTY * OERE.FPRICE) AS DECIMAL(19,2)) AS SALEAMOUNT
         | ,CAST(SUM(OERE.FPRICEQTY * OERE.FTAXPRICE) AS DECIMAL(19,2)) AS SALETAXAMOUNT
         | ,DP.fnumber PROJECTNO
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.ODS_ERP_SALORDER} OES ON IF(OERE.F_PAEZ_Text='',0,OERE.F_PAEZ_Text) = OES.FBILLNO
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OERE.F_PAEZ_BASE2 = DS.FID
         |LEFT JOIN ${TableName.DWD_WRITE_COMPANYNAME} DWC ON DWC.COMPANYNAME = big.F_PAEZ_TEXT1
         |WHERE DC.F_PAEZ_CHECKBOX = 0 AND COALESCE(OER.F_ORDERTYPE,0) <> 1 AND OER.FDOCUMENTSTATUS = 'C' and DS.FNAME = '罗冬冬' AND DP.fnumber = 'DK-DSGSDS-NJSC220920'
         |GROUP BY DS.FNAME
         |	,big.F_PAEZ_TEXT1
         |	,big.F_PAEZ_TEXT2
         | ,OES.F_PAEZ_CHECKBOX
         |	,SUBSTRING(OER.FDATE,1,10)
         |	,OES.F_PAEZ_TEXT2
         |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR DP.fnumber LIKE '%HZXM%') THEN '非自营'
         |		ELSE '自营' END
         |	,CASE WHEN DWP.IS_DSHYW = '是' THEN '电商化业务'
         |		ELSE '非电商化业务' END
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END
         |	,DWC.COMPANYSHORTNAME
         | ,DP.fnumber
         |""".stripMargin).show()


  }






}

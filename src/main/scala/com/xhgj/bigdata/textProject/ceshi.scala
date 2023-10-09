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
    spark.sql(
      s"""
         |SELECT
         |  MAX(from_unixtime(unix_timestamp(OERE.KPDATE, 'yyyy/M/d'),'yyyy-MM-dd')) FDATE,
         |  SUM(OERE.RECAMOUNT) SALETAXAMOUNT,
         |  OERE.PROJECTNAME PROJECTNO,
         |  DP.FNAME PROJECTNAME,
         |  CASE WHEN OERE.PROJECTNAME LIKE '%HZXM%' THEN '非自营'
         |		ELSE '自营' END	PERFORMANCEFORM,
         |  CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END as PROJECTSHORTNAME
         |FROM
         | ${TableName.DWD_HISTORY_RECEIVABLEENTRY} OERE
         | LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.PROJECTNAME = DP.fnumber
         | LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |GROUP BY OERE.PROJECTNAME,DP.FNAME,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END
         |""".stripMargin).createOrReplaceTempView("history_rece")

    //2023年以后的已开票数据
    spark.sql(
      s"""
         |SELECT MIN(SUBSTRING(OER.f_pxdf_date,1,10)) AS BUSINESSDATE	--业务日期
         |	,DP.fnumber	PROJECTNO	--项目编号
         | ,CAST(SUM( OERE.FPRICEQTY * OERE.FTAXPRICE ) AS DECIMAL(19,2)) AS SALETAXAMOUNT --含税总额
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON big.FSALESMAN = DS.FID
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE ((OER.FSETTLEORGID = '2297156' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司')) OR (OER.FSETTLEORGID = '910474' AND DP.FNAME not like '%中核集团%' and DC.FNAME != '咸亨国际电子商务有限公司')) AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'  AND OER.FDOCUMENTSTATUS = 'C'
         |and SUBSTRING(OER.f_pxdf_date,1,4) >= '2023'
         |GROUP BY DP.fnumber
         |""".stripMargin).createOrReplaceTempView("ykp_new")

    //将23年以前以及23年之后的已开票应收金额集合起来

    spark.sql(
      s"""
         |select
         |  COALESCE(hr.PROJECTNO,yn.PROJECTNO,'') PROJECTNO,
         |  COALESCE(hr.SALETAXAMOUNT,0) + COALESCE(yn.SALETAXAMOUNT,0) SALETAXAMOUNT,
         |  COALESCE(hr.FDATE,yn.BUSINESSDATE,'') BUSINESSDATE
         |from history_rece hr full join ykp_new yn on hr.PROJECTNO = yn.PROJECTNO
         |""".stripMargin).createOrReplaceTempView("ykpdata")


    //取当年收款单收款 2297156 DP咸亨国际电子商务有限公司    910474 DP咸亨国际科技股份有限公司
    spark.sql(
      s"""
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME ,SUM(OERE.FRECAMOUNTFOR_E) REAMOUNT
         |FROM ${TableName.ODS_ERP_RECEIVEBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVEBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FPAYUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FPAYORGID = '2297156' AND OER.FDOCUMENTSTATUS = 'C'
         |	AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司') AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         | AND SUBSTRING(OER.FCREATEDATE,1,4) >= '2023'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |UNION ALL
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FRECAMOUNTFOR_E) REAMOUNT
         |FROM ${TableName.ODS_ERP_RECEIVEBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVEBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FPAYUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FPAYORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司' and DC.FNAME != '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团'  AND SUBSTRING(OER.FCREATEDATE,1,4) >= '2023'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |""".stripMargin).createOrReplaceTempView("A6")
    //取当前系统日期前一天年份所有的收款退款单数据
    spark.sql(
      s"""
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FREALREFUNDAMOUNTFOR) AS REAMOUNT
         |FROM ${TableName.ODS_ERP_REFUNDBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_REFUNDBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FRECTUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FPAYORGID = '2297156' AND OER.FDOCUMENTSTATUS = 'C' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司') AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND SUBSTRING(OER.FCREATEDATE,1,4) >= '2023'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |UNION ALL
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FREALREFUNDAMOUNTFOR) AS REAMOUNT
         |FROM ${TableName.ODS_ERP_REFUNDBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_REFUNDBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FRECTUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FPAYORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司' and DC.FNAME != '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团' AND SUBSTRING(OER.FCREATEDATE,1,4) >= '2023'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |""".stripMargin).createOrReplaceTempView("A7")


    spark.sql(
      s"""
         |SELECT
         |	A.PROJECTNO,
         | DWP.PROJECTSHORTNAME,
         |  DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),DATE_FORMAT(SUBSTRING(A.BUSINESSDATE,1,10),'yyyy-MM-dd')) YKPAGING,
         |  case when COALESCE(A.SALETAXAMOUNT,0) = 0 then 0
         |  else if(CAST(COALESCE(A.SALETAXAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) + CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)) < 0, 0,CAST(COALESCE(A.SALETAXAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) + CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)))
         |  end as YKPWHKAMOUNT
         |FROM
         |ykpdata A LEFT JOIN A6 ON A.PROJECTNO = A6.FNUMBER
         |LEFT JOIN A7 ON A.PROJECTNO = A7.FNUMBER
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON A.PROJECTNO = DP.fnumber
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |""".stripMargin)







    //------------------------------------------
    spark.sql(
      s"""
         |SELECT SUBSTRING(OER.f_pxdf_date,1,10) AS BUSINESSDATE	--业务日期
         |	,DP.fnumber	PROJECTNO	--项目编号
         | , OERE.FPRICEQTY * OERE.FTAXPRICE SALETAXAMOUNT --含税总额
         | ,OER.FSETTLEORGID
         | ,DC.FNAME,DP.FNAME,big.F_PAEZ_TEXT1,OER.FDOCUMENTSTATUS,OER.f_pxdf_date
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON big.FSALESMAN = DS.FID
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE DP.fnumber ='DK-DSGSDS-TLHG220001'
         |""".stripMargin).show(10,false)


    spark.sql(
      s"""
         |SELECT
         |* FROM history_rece WHERE PROJECTNO ='DK-DSGSDS-TLHG220001'
         |""".stripMargin).show(10, false)
    spark.sql(
      s"""
         |SELECT
         |* FROM ykp_new WHERE PROJECTNO ='DK-DSGSDS-TLHG220001'
         |""".stripMargin).show(10, false)
    spark.sql(
      s"""
         |SELECT
         |* FROM ykpdata WHERE PROJECTNO ='DK-DSGSDS-TLHG220001'
         |""".stripMargin).show(10,false)
    spark.sql(
      s"""
         |SELECT
         |* FROM A6 WHERE FNUMBER ='DK-DSGSDS-TLHG220001'
         |""".stripMargin).show(10, false)
    spark.sql(
      s"""
         |SELECT
         |* FROM A7 WHERE FNUMBER ='DK-DSGSDS-TLHG220001'
         |""".stripMargin).show(10, false)

//    val table = "ads_test_table"
//    MysqlConnect.overrideTable(table, result2)


  }






}

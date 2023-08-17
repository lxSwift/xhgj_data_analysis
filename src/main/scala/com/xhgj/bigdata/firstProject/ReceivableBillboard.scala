package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{Config, MysqlConnect, TableName}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{DriverManager, Statement}
import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/30 9:23
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: ReceivableBillboard
 * @Description: 应收看板
 * @Version 1.0
 */
object ReceivableBillboard {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job ReceivableBillboard.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
//    salman(spark)
    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession): Unit = {


    /**
     * F_PAEZ_CHECKBOX定向 1代表是定向
     * F_PXDF_TEXT项目编码
     * 2023-05-31 以前(包含2023-05-31)的应收单数据
     * 过滤出审核状态为已审核， 销售组织为DP咸亨国际电子商务有限公司，客户名称不为DP咸亨国际电子商务有限公司，且应付单创建日期小于2023-05-31, 取F_PAEZ_TEXT22销售员所属公司为电商的
     * 并union
     * 过滤出销售组织为DP咸亨国际科技股份有限公司以及F_PAEZ_TEXT22销售员所属公司为咸亨国际电子商务有限公司,项目简称不为中核集团,且应付单创建日期小于2023-05-31的数据
     *
     * 总而言之就是获取了ERP 2023-05-31之前所有的大票订单 电商公司应收款
     */
    spark.sql(
      s"""
        |SELECT DS.FNAME AS SALENAME		--销售员
        |	,MIN(SUBSTRING(OER.FCREATEDATE,1,10)) AS BUSINESSDATE	--业务日期
        |	,DP.fnumber	PROJECTNO	--项目编号
        |	,DP.FNAME PROJECTNAME		--项目名称
        |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
        |		ELSE '自营' END AS PERFORMANCEFORM		--履约形式
        |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
        |		ELSE '其他' END AS PROJECTSHORTNAME		--项目简称
        |	,SUM(OERE.FPRICEQTY * OERE.FTAXPRICE) AS SALEAMOUNT		--金额
        |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
        |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
        |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
        |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
        |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
        |LEFT JOIN ${TableName.ODS_ERP_SALORDER} OES ON IF(OERE.F_PAEZ_Text='',0,OERE.F_PAEZ_Text) = OES.FBILLNO
        |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
        |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OERE.F_PAEZ_BASE2 = DS.FID
        |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FSETTLEORGID = '2297156' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司') AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
        |	AND SUBSTRING(OER.FCREATEDATE,1,10) <= '2023-05-31'
        |GROUP BY DS.FNAME
        |	,DP.fnumber
        |	,DP.FNAME
        |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
        |		ELSE '自营' END
        |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
        |		ELSE '其他' END
        |UNION ALL
        |SELECT DS.FNAME AS SALENAME		--销售员
        |	,MIN(SUBSTRING(OER.FCREATEDATE,1,10)) AS BUSINESSDATE	--业务日期
        |	,DP.fnumber	PROJECTNO		--项目编号
        |	,DP.FNAME PROJECTNAME		--项目名称
        |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
        |		ELSE '自营' END AS PERFORMANCEFORM		--履约形式
        |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
        |		ELSE '其他' END AS PROJECTSHORTNAME		--项目简称
        |	,SUM(OERE.FPRICEQTY * OERE.FTAXPRICE) AS SALEAMOUNT		--金额
        |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
        |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
        |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
        |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
        |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
        |LEFT JOIN ${TableName.ODS_ERP_SALORDER} OES ON IF(OERE.F_PAEZ_Text='',0,OERE.F_PAEZ_Text) = OES.FBILLNO
        |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
        |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OERE.F_PAEZ_BASE2 = DS.FID
        |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FSETTLEORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
        |	AND DWP.PROJECTSHORTNAME != '中核集团' AND SUBSTRING(OER.FCREATEDATE,1,10) <= '2023-05-31'
        |GROUP BY DS.FNAME
        |	,DP.fnumber
        |	,DP.FNAME
        |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
        |		ELSE '自营' END
        |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
        |		ELSE '其他' END
        |""".stripMargin).createOrReplaceTempView("A1")

    //财务提供的截止2023-05-31的应收款手工数据, 过滤掉给销售公司开的数据
    spark.sql(
      s"""
         |SELECT
         |	PROJECTNO,
         |	SALESMAN,
         |	PROJECTNAME,
         |	CASE WHEN INSTR(PROJECTSHORTNAME,'-')> 0 THEN  SUBSTRING(PROJECTSHORTNAME,INSTR(PROJECTSHORTNAME,'-')+1,LENGTH(PROJECTSHORTNAME))
         |		ELSE PROJECTSHORTNAME END AS PROJECTSHORTNAME,
         |	MIN(KPDATE) KPDATE,
         |	SUM(RECAMOUNT) AS RECAMOUNT
         |FROM ${TableName.DWD_HISTORY_RECEIVABLE} DHR
         |where TRIM(DHR.orglevel) != ''
         |GROUP BY
         |	PROJECTNO,
         |	SALESMAN,
         |	PROJECTNAME,
         |	CASE WHEN INSTR(PROJECTSHORTNAME,'-')> 0 THEN SUBSTRING(PROJECTSHORTNAME,INSTR(PROJECTSHORTNAME,'-')+1,LENGTH(PROJECTSHORTNAME))
         |		ELSE PROJECTSHORTNAME END
         |""".stripMargin).createOrReplaceTempView("A2")
    //2023-06-01以后(含2023-06-01)的应收单数据
    spark.sql(
      s"""
         |SELECT DS.FNAME AS SALENAME		--销售员
         |	,MIN(SUBSTRING(OER.F_PXDF_DATE,1,10)) AS BUSINESSDATE	--业务日期(发票日期)
         |	,DP.fnumber	PROJECTNO	--项目编号
         |	,DP.FNAME PROJECTNAME		--项目名称
         |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
         |		ELSE '自营' END AS PERFORMANCEFORM		--履约形式
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END AS PROJECTSHORTNAME		--项目简称
         |	,SUM(OERE.FPRICEQTY * OERE.FTAXPRICE) AS SALEAMOUNT		--金额
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.ODS_ERP_SALORDER} OES ON IF(OERE.F_PAEZ_Text='',0,OERE.F_PAEZ_Text) = OES.FBILLNO
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OERE.F_PAEZ_BASE2 = DS.FID
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FSETTLEORGID = '2297156' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司') AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND SUBSTRING(OER.FCREATEDATE,1,10) >= '2023-06-01'
         |GROUP BY DS.FNAME
         |	,DP.fnumber
         |	,DP.FNAME
         |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
         |		ELSE '自营' END
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END
         |UNION ALL
         |SELECT DS.FNAME AS SALENAME		--销售员
         |	,MIN(SUBSTRING(OER.F_PXDF_DATE,1,10)) AS BUSINESSDATE	--业务日期
         |	,DP.fnumber	PROJECTNO	--项目编号
         |	,DP.FNAME PROJECTNAME		--项目名称
         |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
         |		ELSE '自营' END AS PERFORMANCEFORM		--履约形式
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END AS PROJECTSHORTNAME		--项目简称
         |	,SUM(OERE.FPRICEQTY * OERE.FTAXPRICE) AS SALEAMOUNT		--金额
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.ODS_ERP_SALORDER} OES ON IF(OERE.F_PAEZ_Text='',0,OERE.F_PAEZ_Text) = OES.FBILLNO
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OERE.F_PAEZ_BASE2 = DS.FID
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FSETTLEORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团' AND SUBSTRING(OER.FCREATEDATE,1,10) >= '2023-06-01'
         |GROUP BY DS.FNAME
         |	,DP.fnumber
         |	,DP.FNAME
         |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
         |		ELSE '自营' END
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END
         |""".stripMargin).createOrReplaceTempView("A3")
    //2023-06-01以后(含2023-06-01)的收款单数据
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
         |	AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司') AND SUBSTRING(OER.FCREATEDATE,1,10) >= '2023-06-01' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         | and substring(oer.fdate,1,10) >= '2023-06-01'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |UNION ALL
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FRECAMOUNTFOR_E) REAMOUNT
         |FROM ${TableName.ODS_ERP_RECEIVEBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVEBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FPAYUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FPAYORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团' AND SUBSTRING(OER.FCREATEDATE,1,10) >= '2023-06-01' and substring(oer.fdate,1,10) >= '2023-06-01'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |""".stripMargin).createOrReplaceTempView("A4")
    //2023-06-01以后(含2023-06-01)的收款退款单数据
    spark.sql(
      s"""
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FREALREFUNDAMOUNTFOR)*-1 AS REAMOUNT
         |FROM ${TableName.ODS_ERP_REFUNDBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_REFUNDBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FRECTUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FPAYORGID = '2297156' AND OER.FDOCUMENTSTATUS = 'C' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司') AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND SUBSTRING(OER.FCREATEDATE,1,10) >= '2023-06-01' and substring(oer.fdate,1,10) >= '2023-06-01'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |UNION ALL
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FREALREFUNDAMOUNTFOR)*-1 AS REAMOUNT
         |FROM ${TableName.ODS_ERP_REFUNDBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_REFUNDBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FRECTUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FPAYORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团' AND SUBSTRING(OER.FCREATEDATE,1,10) >= '2023-06-01' and substring(oer.fdate,1,10) >= '2023-06-01'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |""".stripMargin).createOrReplaceTempView("A5")
    //取当年收款单收款
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
         | AND SUBSTRING(OER.FCREATEDATE,1,4) = YEAR(date_sub(current_date(),1))
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |UNION ALL
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FRECAMOUNTFOR_E) REAMOUNT
         |FROM ${TableName.ODS_ERP_RECEIVEBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVEBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FPAYUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FPAYORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团'  AND SUBSTRING(OER.FCREATEDATE,1,4) = YEAR(date_sub(current_date(),1))
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |""".stripMargin).createOrReplaceTempView("A6")
    //取当年收款退款单数据
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
         |	AND SUBSTRING(OER.FCREATEDATE,1,4) = YEAR(date_sub(current_date(),1))
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |UNION ALL
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FREALREFUNDAMOUNTFOR) AS REAMOUNT
         |FROM ${TableName.ODS_ERP_REFUNDBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_REFUNDBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FRECTUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FPAYORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团' AND SUBSTRING(OER.FCREATEDATE,1,4) = YEAR(date_sub(current_date(),1))
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |""".stripMargin).createOrReplaceTempView("A7")
//    初始化
//    val result =spark.sql(
//      s"""
//         |SELECT A1.SALENAME,
//         |	COALESCE(A2.KPDATE,A1.BUSINESSDATE) AS BUSINESSDATE,
//         |	COALESCE(A2.PROJECTNO,A1.PROJECTNO) AS PROJECTNO,
//         |	A1.PROJECTNAME,
//         |	A1.PERFORMANCEFORM,
//         |	A1.PROJECTSHORTNAME,
//         |	CAST(A1.SALEAMOUNT AS DECIMAL(19,2)) SALEAMOUNT,
//         |	NULL AS PAYBACKAMOUNT,
//         |	NULL AS REFAMOUNT,
//         |	CAST(A2.RECAMOUNT AS DECIMAL(19,2)) RECAMOUNT,
//         |	DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),DATE_FORMAT(A2.KPDATE,'yyyy-MM-dd')) AGING,
//         | '2023-05' UPDATEMONTH
//         |FROM A1
//         |FULL JOIN A2 ON A2.PROJECTNO = A1.PROJECTNO
//         |UNION ALL
//         |SELECT A3.SALENAME,
//         |	A3.BUSINESSDATE,
//         |	A3.PROJECTNO,
//         |	A3.PROJECTNAME,
//         |	A3.PERFORMANCEFORM,
//         |	A3.PROJECTSHORTNAME,
//         |	CAST(A3.SALEAMOUNT AS DECIMAL(19,2)) SALEAMOUNT,
//         |	CAST(A4.REAMOUNT AS DECIMAL(19,2)) AS PAYBACKAMOUNT,
//         |	CAST(A5.REAMOUNT AS DECIMAL(19,2)) AS REFAMOUNT,
//         |	CAST(A3.SALEAMOUNT AS DECIMAL(19,2)) - CAST(A4.REAMOUNT AS DECIMAL(19,2)) - CAST(A5.REAMOUNT AS DECIMAL(19,2)) AS RECAMOUNT,
//         |	DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),DATE_FORMAT(A3.BUSINESSDATE,'yyyy-MM-dd')) AGING,
//         | '2023-05' UPDATEMONTH
//         |FROM A3
//         |LEFT JOIN A4 ON A3.PROJECTNO = A4.FNUMBER
//         |LEFT JOIN A5 ON A3.PROJECTNO = A5.FNUMBER
//         |""".stripMargin)
    /**
     * A1 6月前历史应收款
     * A2 2023-05-31之前的应收款手工数据
     * A3 2023-06-01以后(含2023-06-01)的应收单数据
     * A4 2023-06-01以后(含2023-06-01)的收款单数据
     * A5 2023-06-01以后(含2023-06-01)的收款退款单数据
     * A6 取当年收款单收款
     * A7 取当年收款退款单数据
     */
    val result: DataFrame = spark.sql(
      s"""
         |SELECT
         |	COALESCE(A2.SALESMAN,A1.SALENAME) SALENAME,
         |	COALESCE(A2.KPDATE,A1.BUSINESSDATE) AS BUSINESSDATE,
         |	COALESCE(A2.PROJECTNO,A1.PROJECTNO) AS PROJECTNO,
         |	COALESCE(A1.PROJECTNAME,A2.PROJECTNAME) AS PROJECTNAME,
         |	A1.PERFORMANCEFORM,
         |	COALESCE(A1.PROJECTSHORTNAME,A2.PROJECTSHORTNAME) AS PROJECTSHORTNAME,
         |	A1.SALEAMOUNT,
         |	A4.REAMOUNT AS PAYBACKAMOUNT,
         |	A5.REAMOUNT AS REFAMOUNT,
         |	CAST(COALESCE(A2.RECAMOUNT,0) AS DECIMAL(19,2))-CAST(COALESCE(A4.REAMOUNT,0) AS DECIMAL(19,2))-CAST(COALESCE(A5.REAMOUNT,0) AS DECIMAL(19,2)) AS RECAMOUNT,
         |	DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),DATE_FORMAT(A2.KPDATE,'yyyy-MM-dd')) AGING, --账龄
         |	SUBSTRING(DATE_ADD(CURRENT_TIMESTAMP() ,-1),1,7) AS UPDATEMONTH,
         |	CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)) AS THEYEARAMOUNT
         |FROM A1
         |FULL JOIN A2 ON A2.PROJECTNO = A1.PROJECTNO
         |LEFT JOIN A4 ON A2.PROJECTNO = A4.FNUMBER
         |LEFT JOIN A5 ON A2.PROJECTNO = A5.FNUMBER
         |LEFT JOIN A6 ON A1.PROJECTNO = A6.FNUMBER
         |LEFT JOIN A7 ON A1.PROJECTNO = A7.FNUMBER
         |UNION ALL
         |SELECT
         |	A3.SALENAME,
         |	A3.BUSINESSDATE,
         |	A3.PROJECTNO,
         |	A3.PROJECTNAME,
         |	A3.PERFORMANCEFORM,
         |	A3.PROJECTSHORTNAME,
         |	A3.SALEAMOUNT,
         |	A4.REAMOUNT AS PAYBACKAMOUNT,
         |	A5.REAMOUNT AS REFAMOUNT,
         |	CAST(COALESCE(A3.SALEAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A4.REAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A5.REAMOUNT,0) AS DECIMAL(19,2)) AS RECAMOUNT,
         |	DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),DATE_FORMAT(A3.BUSINESSDATE,'yyyy-MM-dd')) AGING,
         |	SUBSTRING(DATE_ADD(CURRENT_TIMESTAMP() ,-1),1,7) AS UPDATEMONTH,
         |	CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)) AS THEYEARAMOUNT
         |FROM A3
         |LEFT JOIN A4 ON A3.PROJECTNO = A4.FNUMBER
         |LEFT JOIN A5 ON A3.PROJECTNO = A5.FNUMBER
         |LEFT JOIN A6 ON A3.PROJECTNO = A6.FNUMBER
         |LEFT JOIN A7 ON A3.PROJECTNO = A7.FNUMBER
         |""".stripMargin)

    println("result:"+result.count())

//    val table = "ads_fin_receivableboard"
//    MysqlConnect.overrideTable(table,result)


    //先删除前一天所属月份的数据
    val deleteQuery = s"DELETE FROM ads_xhgj.ads_fin_receivableboard WHERE UPDATEMONTH = DATE_FORMAT(CURRENT_DATE() - INTERVAL 1 DAY, '%Y-%m')"
    MysqlConnect.executeUpdateTable(deleteQuery)
    //导出至mysql库
    val table = "ads_fin_receivableboard"
    MysqlConnect.appendTable(table,result)




  }

  def salman(spark:SparkSession)={
    val result = spark.sql(
      s"""
         |SELECT DS.FNAME AS SALENAME		--销售员
         |	,OERE.F_PXDF_TEXT	PROJECTNO	--项目编号
         |	,OERE.F_PXDF_TEXT1 PROJECTNAME		--项目名称
         | ,OER.F_PAEZ_TEXT22 --销售员所属公司
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OERE.F_PAEZ_BASE2 = DS.FID
         |WHERE OER.FDOCUMENTSTATUS = 'C'
         |GROUP BY DS.FNAME
         |	,OERE.F_PXDF_TEXT
         |	,OERE.F_PXDF_TEXT1,
         | OER.F_PAEZ_TEXT22
         """.stripMargin)

    val table = "ads_fin_salman"
    MysqlConnect.overrideTable(table, result)
  }
}

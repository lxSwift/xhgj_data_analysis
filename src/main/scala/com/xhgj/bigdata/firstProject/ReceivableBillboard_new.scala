package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{Config, MysqlConnect, TableName}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

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
object ReceivableBillboard_new {
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
     * 2023的应收单数据
     * 过滤出审核状态为已审核， 销售组织为咸亨国际电子商务有限公司，客户名称不为咸亨国际电子商务有限公司和DP咸亨国际科技股份有限公司, 取大票项目单F_PAEZ_TEXT1销售员所属公司为电商的
     * 并union
     * 过滤出销售组织为DP咸亨国际科技股份有限公司以及大票项目单F_PAEZ_TEXT1销售员所属公司为咸亨国际电子商务有限公司,项目简称不为中核集团,且应付单创建日期小于2023-05-31的数据
     * 910473咸亨国际科技股份有限公司  2297171咸亨国际电子商务有限公司
     * 总而言之就是获取了ERP 今年所有的小票订单 电商公司应收款
     */

    //截止目前所有的已发货含税金额
    spark.sql(
      s"""
         |SELECT
         |  DS.FNAME SALENAME
         |	,MIN(SUBSTRING(OER.FDATE,1,10)) AS BUSINESSDATE	--业务日期
         |	,DP.fnumber	PROJECTNO	--项目编号
         |	,DP.FNAME PROJECTNAME		--项目名称
         |	,CASE WHEN DP.fnumber LIKE '%HZXM%' THEN '非自营'
         |		ELSE '自营' END	PERFORMANCEFORM	--履约形式
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END AS PROJECTSHORTNAME		--项目简称
         | ,CAST(SUM(case when DP.FBEHALFINVOICERATIO is not null and TRIM(DP.FBEHALFINVOICERATIO) != '' and TRIM(DP.FBEHALFINVOICERATIO) != 0 then OERE.FPRICEQTY * OERE.FTAXPRICE/DP.FBEHALFINVOICERATIO*100
         | else OERE.FPRICEQTY * OERE.FTAXPRICE end) AS DECIMAL(19,2)) AS SALETAXAMOUNT --含税总额
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON big.FSALESMAN = DS.FID
         |LEFT JOIN ${TableName.ODS_ERP_SALORDER} OES ON IF(OERE.F_PAEZ_Text='',0,OERE.F_PAEZ_Text) = OES.FBILLNO
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |LEFT JOIN ${TableName.DWD_WRITE_COMPANYNAME} DWC ON DWC.COMPANYNAME = big.F_PAEZ_TEXT1
         |WHERE ((OER.FSETTLEORGID = '2297171' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司')) OR (OER.FSETTLEORGID = '910473' AND DP.FNAME not like '%中核集团%' and DC.FNAME != '咸亨国际电子商务有限公司')) AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'  AND OER.FDOCUMENTSTATUS = 'C'
         |GROUP BY
         |	DS.FNAME,DP.fnumber
         |	,DP.FNAME
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END
         |""".stripMargin).createOrReplaceTempView("receive")


    /**
     * 获取2022-12-31日期以前的已开票应收款余额
     */
    spark.sql(
      s"""
         |SELECT
         |  MAX(from_unixtime(unix_timestamp(OERE.KPDATE, 'yyyy/M/dd'),'yyyy-MM-dd')) FDATE,
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


    /**
     *获取2023年以前的已发货
     */
    spark.sql(
      s"""
         |SELECT MIN(SUBSTRING(OER.FDATE,1,10)) AS BUSINESSDATE	--业务日期
         |	,DP.fnumber	PROJECTNO	--项目编号
         | ,CAST(SUM(case when DP.FBEHALFINVOICERATIO is not null and TRIM(DP.FBEHALFINVOICERATIO) != '' and TRIM(DP.FBEHALFINVOICERATIO) != 0 then OERE.FPRICEQTY * OERE.FTAXPRICE/DP.FBEHALFINVOICERATIO*100
         | else OERE.FPRICEQTY * OERE.FTAXPRICE end) AS DECIMAL(19,2)) AS SALETAXAMOUNT --含税总额
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |WHERE ((OER.FSETTLEORGID = '2297171' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司')) OR (OER.FSETTLEORGID = '910473' AND DP.FNAME not like '%中核集团%' and DC.FNAME != '咸亨国际电子商务有限公司')) AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'  AND OER.FDOCUMENTSTATUS = 'C'
         |and SUBSTRING(OER.FDATE,1,4) < '2023'
         |GROUP BY DP.fnumber
         |""".stripMargin).createOrReplaceTempView("all_fhrece")

    /**
     * 获取已开票口径2023以前所有数据
     */
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
         |WHERE ((OER.FSETTLEORGID = '2297156' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司')) OR (OER.FSETTLEORGID = '910474' AND DP.FNAME not like '%中核集团%' and DC.FNAME != '咸亨国际电子商务有限公司')) AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'  AND OER.FDOCUMENTSTATUS = 'C'
         |AND SUBSTRING(OER.f_pxdf_date,1,4) < '2023'
         |GROUP BY DP.fnumber
         |""".stripMargin).createOrReplaceTempView("kaipiao")



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


    //历史所有已开票数据
    val result2 = spark.sql(
      s"""
         |SELECT DS.FNAME SALENAME
         |,MIN(SUBSTRING(OER.f_pxdf_date,1,10)) AS BUSINESSDATE	--业务日期
         |	,DP.fnumber	PROJECTNO	--项目编号
         | ,DP.fname PROJECTNAME
         | ,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END AS PROJECTSHORTNAME		--项目简称
         | ,CAST(SUM( OERE.FPRICEQTY * OERE.FTAXPRICE ) AS DECIMAL(19,2)) AS SALETAXAMOUNT --含税总额
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON big.FSALESMAN = DS.FID
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE ((OER.FSETTLEORGID = '2297156' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司')) OR (OER.FSETTLEORGID = '910474' AND DP.FNAME not like '%中核集团%' and DC.FNAME != '咸亨国际电子商务有限公司')) AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'  AND OER.FDOCUMENTSTATUS = 'C'
         |GROUP BY DS.FNAME,DP.fnumber,DP.fname,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END
         |""".stripMargin)
    /**
     * receive 2023年发货
     * history_rece 手工账
     * kaipiao 2022已开票
     * all_fhrece 所有已发货
     * A6 收款单
     * A7 收款退款单
     */

    //关联的上的,代表2022年开票但是有延迟发货的情况需要处理
    spark.sql(
      s"""
         |SELECT
         |  b1.BUSINESSDATE	--业务日期
         |	,b1.PROJECTNO	--项目编号
         |	,b1.PROJECTNAME		--项目名称
         |	,b1.PERFORMANCEFORM	--履约形式
         |	,b1.PROJECTSHORTNAME		--项目简称
         | ,case when (b1.SALETAXAMOUNT-ifnull(b2.SALETAXAMOUNT,0) < 1) and ifnull(b3.SALETAXAMOUNT,0) = 0 then 0
         | when (b1.SALETAXAMOUNT-ifnull(b2.SALETAXAMOUNT,0) between -1 and 1) and ifnull(b3.SALETAXAMOUNT,0) > 0 then b1.SALETAXAMOUNT-ifnull(b2.SALETAXAMOUNT,0)+ ifnull(b3.SALETAXAMOUNT,0)
         | when (b1.SALETAXAMOUNT-ifnull(b2.SALETAXAMOUNT,0) < -1) and ifnull(b3.SALETAXAMOUNT,0) > 0 then b1.SALETAXAMOUNT
         | when (b1.SALETAXAMOUNT-ifnull(b2.SALETAXAMOUNT,0) > 1) then b1.SALETAXAMOUNT-ifnull(b2.SALETAXAMOUNT,0)+ ifnull(b3.SALETAXAMOUNT,0) end as SALETAXAMOUNT
         |FROM receive b1
         |JOIN kaipiao b2 ON b1.PROJECTNO = b2.PROJECTNO
         |LEFT JOIN history_rece b3 ON b1.PROJECTNO = b3.PROJECTNO
         |""".stripMargin).createOrReplaceTempView("result1")

    //取出2022年本年完成已开票已发货的数据,并取发货日期
    spark.sql(
      s"""
         |select
         |  *
         | from
         |result1
         |union all
         |SELECT
         |  BUSINESSDATE	--业务日期
         |	,	PROJECTNO	--项目编号
         |	, PROJECTNAME		--项目名称
         |	,	PERFORMANCEFORM	--履约形式
         |	, PROJECTSHORTNAME		--项目简称
         | , SALETAXAMOUNT --含税总额
         |from
         |  receive
         |WHERE PROJECTNO NOT IN (SELECT PROJECTNO FROM result1)
         |""".stripMargin).createOrReplaceTempView("res2")

    result2.createOrReplaceTempView("ykpdata")

    val result = spark.sql(
      s"""
         |SELECT
         |  DS.FNAME SALENAME,
         |	A.BUSINESSDATE,
         |	A.PROJECTNO,
         |	A.PROJECTNAME,
         |	A.PERFORMANCEFORM,
         |	A.PROJECTSHORTNAME,
         |	A.SALETAXAMOUNT,
         |  A6.REAMOUNT PAYBACKAMOUNT,
         |  A7.REAMOUNT REFAMOUNT,
         |  if(CAST(COALESCE(A.SALETAXAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) + CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)) < 0, 0,CAST(COALESCE(A.SALETAXAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) + CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2))) AS RECAMOUNT,
         |  DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),DATE_FORMAT(A.BUSINESSDATE,'yyyy-MM-dd')) AGING,
         |  SUBSTRING(DATE_ADD(CURRENT_TIMESTAMP() ,-1),1,7) AS UPDATEMONTH,
         |  CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)) AS THEYEARAMOUNT,
         |  SUBSTRING(KP.BUSINESSDATE,1,10) KPDATE,
         |  DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),DATE_FORMAT(SUBSTRING(KP.BUSINESSDATE,1,10),'yyyy-MM-dd')) YKPAGING,
         |  case when COALESCE(KP.SALETAXAMOUNT,0) = 0 then 0
         |  else if(CAST(COALESCE(A.SALETAXAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) + CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)) < 0, 0,CAST(COALESCE(A.SALETAXAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) + CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)))
         |  end as YKPWHKAMOUNT
         |FROM
         |res2 A LEFT JOIN A6 ON A.PROJECTNO = A6.FNUMBER
         |LEFT JOIN A7 ON A.PROJECTNO = A7.FNUMBER
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on A.PROJECTNO= big.fbillno
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON big.FSALESMAN = DS.FID
         |left join ykpdata KP ON A.PROJECTNO = KP.PROJECTNO
         |""".stripMargin)
    val table = "ads_fin_receivableboardnew"

    MysqlConnect.overrideTable(table, result)

    //历史所有已开票数据

    val table2 = "ads_fin_ykpalldata"
    MysqlConnect.overrideTable(table2, result2)

    //已发货口径的数据，所有日期的数据
    val result3 =spark.sql(
      s"""
         |select
         |  *
         |from
         |receive
         |""".stripMargin)
    val table3 = "ads_fin_yfhalldata"
    MysqlConnect.overrideTable(table3, result3)


    //取当年收款以及近三月收款所需数据
    //取当年收款单收款 2297156 DP咸亨国际电子商务有限公司    910474 DP咸亨国际科技股份有限公司
    spark.sql(
      s"""
         |SELECT DS.FNAME SALENAME,DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME ,SUM(OERE.FRECAMOUNTFOR_E) REAMOUNT,SUBSTRING(OER.FCREATEDATE,1,10) FCREATEDATE
         |FROM ${TableName.ODS_ERP_RECEIVEBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVEBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FPAYUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON big.FSALESMAN = DS.FID
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FPAYORGID = '2297156' AND OER.FDOCUMENTSTATUS = 'C'
         |	AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司') AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         | AND SUBSTRING(OER.FCREATEDATE,1,4) >= '2023'
         |GROUP BY DS.FNAME,DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUBSTRING(OER.FCREATEDATE,1,10)
         |UNION ALL
         |SELECT DS.FNAME SALENAME,DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FRECAMOUNTFOR_E) REAMOUNT,SUBSTRING(OER.FCREATEDATE,1,10) FCREATEDATE
         |FROM ${TableName.ODS_ERP_RECEIVEBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVEBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FPAYUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON big.FSALESMAN = DS.FID
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FPAYORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司' and DC.FNAME != '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团'  AND SUBSTRING(OER.FCREATEDATE,1,4) >= '2023'
         |GROUP BY DS.FNAME,DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUBSTRING(OER.FCREATEDATE,1,10)
         |""".stripMargin).createOrReplaceTempView("A8")
    //取当前系统日期前一天年份所有的收款退款单数据
    spark.sql(
      s"""
         |SELECT DS.FNAME SALENAME,DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FREALREFUNDAMOUNTFOR) AS REAMOUNT,SUBSTRING(OER.FCREATEDATE,1,10) FCREATEDATE
         |FROM ${TableName.ODS_ERP_REFUNDBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_REFUNDBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FRECTUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON big.FSALESMAN = DS.FID
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FPAYORGID = '2297156' AND OER.FDOCUMENTSTATUS = 'C' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司') AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND SUBSTRING(OER.FCREATEDATE,1,4) >= '2023'
         |GROUP BY DS.FNAME,DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUBSTRING(OER.FCREATEDATE,1,10)
         |UNION ALL
         |SELECT DS.FNAME SALENAME,DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FREALREFUNDAMOUNTFOR) AS REAMOUNT,SUBSTRING(OER.FCREATEDATE,1,10) FCREATEDATE
         |FROM ${TableName.ODS_ERP_REFUNDBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_REFUNDBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FRECTUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON big.FSALESMAN = DS.FID
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FPAYORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司' and DC.FNAME != '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团' AND SUBSTRING(OER.FCREATEDATE,1,4) >= '2023'
         |GROUP BY DS.FNAME,DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUBSTRING(OER.FCREATEDATE,1,10)
         |""".stripMargin).createOrReplaceTempView("A9")

    val result4 = spark.sql(
      s"""
         |SELECT
         |  COALESCE(A8.FNUMBER,A9.FNUMBER) FNUMBER,COALESCE(A8.PROJECTSHORTNAME,A9.PROJECTSHORTNAME) PROJECTSHORTNAME,
         |  COALESCE(A8.FCREATEDATE,A9.FCREATEDATE) FCREATEDATE,COALESCE(A8.SALENAME,A9.SALENAME) SALENAME,
         |  IFNULL(A8.REAMOUNT,0)-IFNULL(A9.REAMOUNT,0) AS REAMOUNT
         |FROM
         |  A8
         |FULL JOIN A9 ON A8.FNUMBER = A9.FNUMBER AND A8.FCREATEDATE = A9.FCREATEDATE
         |""".stripMargin)
    val table4 = "ads_fin_receamount"
    MysqlConnect.overrideTable(table4, result4)



  }

}

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
    spark.sql(
      s"""
         |SELECT DS.FNAME AS SALENAME		--销售员
         |	,MIN(SUBSTRING(OER.FDATE,1,10)) AS BUSINESSDATE	--业务日期
         |	,DP.fnumber	PROJECTNO	--项目编号
         |	,DP.FNAME PROJECTNAME		--项目名称
         |	,CASE WHEN OES.F_PAEZ_CHECKBOX = 1 then '自营'
         | WHEN DP.fnumber LIKE '%HZXM%' THEN '非自营'
         |		ELSE '自营' END	PERFORMANCEFORM	--履约形式
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END AS PROJECTSHORTNAME		--项目简称
         |	,CAST(SUM(case when DP.FBEHALFINVOICERATIO is not null and TRIM(DP.FBEHALFINVOICERATIO) != '' and TRIM(DP.FBEHALFINVOICERATIO) != 0 then OERE.FPRICEQTY * OERE.FPRICE/DP.FBEHALFINVOICERATIO*100
         | else OERE.FPRICEQTY * OERE.FPRICE end) AS DECIMAL(19,2)) AS SALEAMOUNT --不含税总额
         | ,CAST(SUM(case when DP.FBEHALFINVOICERATIO is not null and TRIM(DP.FBEHALFINVOICERATIO) != '' and TRIM(DP.FBEHALFINVOICERATIO) != 0 then OERE.FPRICEQTY * OERE.FTAXPRICE/DP.FBEHALFINVOICERATIO*100
         | else OERE.FPRICEQTY * OERE.FTAXPRICE end) AS DECIMAL(19,2)) AS SALETAXAMOUNT --含税总额
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.ODS_ERP_SALORDER} OES ON IF(OERE.F_PAEZ_Text='',0,OERE.F_PAEZ_Text) = OES.FBILLNO
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OERE.F_PAEZ_BASE2 = DS.FID
         |LEFT JOIN ${TableName.DWD_WRITE_COMPANYNAME} DWC ON DWC.COMPANYNAME = big.F_PAEZ_TEXT1
         |WHERE ((OER.FSETTLEORGID = '2297171' AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司')) OR (OER.FSETTLEORGID = '910473' AND DP.FNAME not like '%中核集团%')) AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'  AND OER.FDOCUMENTSTATUS = 'C'
         |and substring(OER.FDATE,1,4) = YEAR(DATE_SUB(CURRENT_DATE(), 1)) and  substring(OER.FDATE,1,7) < '2023-08'
         |GROUP BY DS.FNAME
         |	,DP.fnumber
         |	,DP.FNAME
         |	,CASE WHEN OES.F_PAEZ_CHECKBOX = 1 then '自营'
         | WHEN DP.fnumber LIKE '%HZXM%' THEN '非自营'
         |		ELSE '自营' END
         |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
         |		ELSE '其他' END
         |""".stripMargin).createOrReplaceTempView("receive")

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
         | AND SUBSTRING(OER.FCREATEDATE,1,4) = YEAR(date_sub(current_date(),1)) and substring(OER.FCREATEDATE,1,7) < '2023-08'
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
         |	AND DWP.PROJECTSHORTNAME != '中核集团'  AND SUBSTRING(OER.FCREATEDATE,1,4) = YEAR(date_sub(current_date(),1)) and substring(OER.FCREATEDATE,1,7) < '2023-08'
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
         |	AND SUBSTRING(OER.FCREATEDATE,1,4) = YEAR(date_sub(current_date(),1)) and substring(OER.FCREATEDATE,1,7) < '2023-08'
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
         |	AND DWP.PROJECTSHORTNAME != '中核集团' AND SUBSTRING(OER.FCREATEDATE,1,4) = YEAR(date_sub(current_date(),1)) and substring(OER.FCREATEDATE,1,7) < '2023-08'
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
    spark.sql(
      s"""
         |SELECT
         |	A1.SALENAME SALENAME,
         |	A1.BUSINESSDATE AS BUSINESSDATE,
         |	A1.PROJECTNO AS PROJECTNO,
         |	A1.PROJECTNAME AS PROJECTNAME,
         |	A1.PERFORMANCEFORM,
         |	A1.PROJECTSHORTNAME AS PROJECTSHORTNAME,
         |	A1.SALETAXAMOUNT,
         |	CAST(COALESCE(A1.SALETAXAMOUNT,0) AS DECIMAL(19,2))-CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2))-CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)) AS RECAMOUNT,
         |	DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),DATE_FORMAT(A1.BUSINESSDATE,'yyyy-MM-dd')) AGING, --账龄
         |	SUBSTRING(DATE_ADD(CURRENT_TIMESTAMP() ,-1),1,7) AS UPDATEMONTH,
         |	CAST(COALESCE(A6.REAMOUNT,0) AS DECIMAL(19,2)) - CAST(COALESCE(A7.REAMOUNT,0) AS DECIMAL(19,2)) AS THEYEARAMOUNT
         |FROM receive A1
         |LEFT JOIN A6 ON A1.PROJECTNO = A6.FNUMBER
         |LEFT JOIN A7 ON A1.PROJECTNO = A7.FNUMBER
         |""".stripMargin).createOrReplaceTempView("result")

    spark.sql(
      s"""
         |select
         | sum(RECAMOUNT) RECAMOUNT
         |from
         | result
         |""".stripMargin).show(10,false)






  }

}

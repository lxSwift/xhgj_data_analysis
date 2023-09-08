package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{Config, TableName}
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/16 16:13
 * @PackageName:com.xhgj.bigdata.textProject
 * @ClassName: ResTest
 * @Description: TODO
 * @Version 1.0
 */
object ProcureMonitor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job ProcureMonitor.scala")
      .enableHiveSupport()
      .getOrCreate()

//    runRES(spark)
    takeTime(spark)
    purAmount(spark)
    RerurnStock(spark)
    WorkLoad(spark)

    //关闭SparkSession
    spark.stop()
  }

  /**
   * 采购过程--各个阶段花费的时间
   * @param spark
   */
  def takeTime(spark: SparkSession): Unit = {

    /**
     * 将采购申请单表以及其衍生表和维度表关联起来, 并且只需要申请组织为1即万聚公司的值(这个是去组织维度表查到的对应)
     */
    spark.sql(
      s"""
         |SELECT
         | 	OER.FBILLNO,
         | 	OER.FCREATEDATE,
         | 	OER.FAPPROVEDATE,
         | 	OERE.FMATERIALID,
         | 	CASE WHEN OER.FDOCUMENTSTATUS = 'Z' THEN '暂存'
         | 		 WHEN OER.FDOCUMENTSTATUS = 'A' THEN '创建'
         | 		 WHEN OER.FDOCUMENTSTATUS = 'B' THEN '审核中'
         | 		 WHEN OER.FDOCUMENTSTATUS = 'C' THEN '已审核'
         | 		 WHEN OER.FDOCUMENTSTATUS = 'D' THEN '重新审核'
         | 	 		 ELSE OER.FDOCUMENTSTATUS END AS FDOCUMENTSTATUS,
         | 	 	DO1.FNAME REQORGNAME,
         | 	 	OER.FAPPLICATIONDATE ,
         | 	 	OERE.F_PXDF_TEXT1 F_PROJECTNO,
         | 	 	DP.FNAME PROJECTNAME,
         | 	 	DM.FNUMBER MATERNO,
         | 	 	DM.FNAME MATERNAME,
         | 	 	DM.F_PAEZ_TEXT BRANDNAME,
         | 	 	DM.FSPECIFICATION SPECIFICATION,
         | 		CASE WHEN OER.F_PXDF_ORGID = OER.FAPPLICATIONORGID THEN DCT.FNAME
         | 			ELSE DO2.FNAME END AS SALEORGNAME,
         | 		DS.FNAME SALER ,
         | 		OERR.FSRCBILLNO,
         | 		OER.FID,
         | 		OERE.FENTRYID,
         | 		OERL.FSBILLID ,
         | 		OERL.FSID,
         | 		DC.FNAME KHLB,
         | 		IF(OERE.FAPPROVEQTY='',NULL,OERE.FAPPROVEQTY) AS REQQTY
         | FROM ${TableName.ODS_ERP_REQUISITION} OER
         | LEFT JOIN ${TableName.ODS_ERP_REQENTRY} OERE ON OER.FID = OERE.FID
         | LEFT JOIN ${TableName.ODS_ERP_REQENTRY_R} OERR ON OERE.FENTRYID = OERR.FENTRYID
         | LEFT JOIN ${TableName.ODS_ERP_REQENTRY_LK} OERL ON OERE.FENTRYID = OERL.FENTRYID
         | LEFT JOIN ${TableName.DIM_MATERIAL} DM ON OERE.FMATERIALID = DM.FMATERIALID --物料表
         | LEFT JOIN ${TableName.DIM_CUST100501} DC ON DM.F_KHR = DC.FID
         | LEFT JOIN ${TableName.DIM_ORGANIZATIONS} DO1 ON OER.FAPPLICATIONORGID = DO1.FORGID
         | LEFT JOIN ${TableName.DIM_ORGANIZATIONS} DO2 ON OER.F_PXDF_ORGID = DO2.FORGID
         | LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OERE.F_PAEZ_BASE1 = DS.FID
         | LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.F_PROJECTNO = DP.FID
         | LEFT JOIN ${TableName.DIM_CUSTOMER} DCT ON OERE.F_PAEZ_BASE = DCT.FCUSTID
         | WHERE OER.FAPPLICATIONORGID IN ('1','481351') AND (OERE.FMRPTERMINATESTATUS != 'B' OR OERR.FREMAINQTY != OERE.FREQQTY)
         |""".stripMargin).createOrReplaceTempView("A1")

    /**
     * 获取职员的名称和部门信息
     */
    spark.sql(
      s"""
         |SELECT STAFFNAME,DEPTNAME FROM (
         |SELECT DS.FNAME STAFFNAME,
         |	DD.FNAME DEPTNAME,
         |	ROW_NUMBER() OVER(PARTITION BY DS.FNAME ORDER BY DD.FCREATEDATE DESC) RN
         |FROM ${TableName.DIM_STAFF} DS
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} DD ON DS.FDEPTID = DD.FDEPTID
         |WHERE DS.FUSEORGID = 1
         |) DSDD WHERE RN = 1
         |""".stripMargin).createOrReplaceTempView("STAFF")
    /**
     * 采购订单筛选出万聚的以及单号源单号不为空的数据
     */
    spark.sql(
      s"""
        |SELECT '采购订单' AS BILLTYPES,
        | 	OEP.FBILLNO,
        |	OEP.FCREATEDATE,
        |	OEP.FAPPROVEDATE,
        |	OEPE.FMATERIALID,
        |	OEPR.FSRCBILLNO,
        |	CASE WHEN OEP.FDOCUMENTSTATUS = 'Z' THEN '暂存'
        | 		 WHEN OEP.FDOCUMENTSTATUS = 'A' THEN '创建'
        | 		 WHEN OEP.FDOCUMENTSTATUS = 'B' THEN '审核中'
        | 		 WHEN OEP.FDOCUMENTSTATUS = 'C' THEN '已审核'
        | 		 WHEN OEP.FDOCUMENTSTATUS = 'D' THEN '重新审核'
        | 		 ELSE OEP.FDOCUMENTSTATUS END AS FDOCUMENTSTATUS,
        |	DS.FNAME SUPPLIERNAME ,
        |	STAFF.DEPTNAME PURDEPTNMEA,
        |	STAFF.STAFFNAME PURCHASERNAME,
        |	OEPL.FSBILLID,
        |	OEPL.FSID,
        |	OEP.FID,
        |	OEPE.FENTRYID,
        |	IF(OEPE.FQTY='',NULL,OEPE.FQTY) AS PURQTY
        |FROM ${TableName.ODS_ERP_POORDER} OEP
        |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY} OEPE ON OEP.FID = OEPE.FID
        |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY_R} OEPR ON OEPE.FENTRYID = OEPR.FENTRYID
        |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY_LK} OEPL ON OEPL.FENTRYID = OEPE.FENTRYID
        |LEFT JOIN ${TableName.DIM_USER} DU ON OEP.FCREATORID = DU.FUSERID
        |LEFT JOIN STAFF ON STAFF.STAFFNAME = DU.FNAME
        |LEFT JOIN ${TableName.DIM_SUPPLIER} DS ON OEP.FSUPPLIERID = DS.FSUPPLIERID
        | WHERE OEP.FBILLNO <> '' AND OEPR.FSRCBILLNO <> '' AND OEP.FPURCHASEORGID IN ('1','481351')
        |""".stripMargin).createOrReplaceTempView("A2")

    /**
     * 采购入库单获得收料组织为1万聚且单据以及源单编码不为空
     */
    spark.sql(
      s"""
        |SELECT OEI.FBILLNO,
        |	OEI.FCREATEDATE,
        |	OEI.FAPPROVEDATE,
        |	OEIE.FMATERIALID,
        |	OEIE.FSRCBILLNO,
        |	DL.FNAME FLOTNAME,
        |	DS.FNAME STOCKNAME,
        |	OEIL.FSBILLID,
        |	OEIL.FSID,
        |	IF(OEIE.FREALQTY='',NULL,OEIE.FREALQTY) AS INSTOCKQTY
        |FROM ${TableName.ODS_ERP_INSTOCK} OEI
        |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} OEIE ON OEI.FID = OEIE.FID
        |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_LK} OEIL ON OEIE.FENTRYID = OEIL.FENTRYID
        |LEFT JOIN ${TableName.DIM_LOTMASTER} DL ON OEIE.FLOT = DL.FLOTID
        |LEFT JOIN ${TableName.DIM_STOCK} DS ON OEIE.FSTOCKID = DS.FSTOCKID
        |WHERE OEI.FBILLNO <> '' AND OEIE.FSRCBILLNO <>'' AND OEI.FSTOCKORGID IN ('1','481351')
        |""".stripMargin).createOrReplaceTempView("A3")


    spark.sql(
      s"""
         |SELECT
         | 	A2.FBILLNO AS PURNO,
         | 	A2.FDOCUMENTSTATUS AS PURSTATUS,
         | 	A2.SUPPLIERNAME,
         | 	A2.PURDEPTNMEA,
         | 	A2.PURCHASERNAME,
         | 	A2.FCREATEDATE AS PURCREADATE,
         | 	A2.FAPPROVEDATE AS PURAPPDATE,
         |	A2.FSBILLID,
         |	A2.FSID,
         |	A2.FID,
         |	A2.FENTRYID,
         | 	A3.FCREATEDATE AS INCREADATE,
         | 	A3.FAPPROVEDATE AS INAPPDATE,
         | 	A3.FBILLNO AS INSTOCKNO,
         | 	A3.FLOTNAME AS INSTOCKFLOT,
         | 	A3.FMATERIALID,
         | 	A3.STOCKNAME AS INSTOCK,
         |	COALESCE (A3.INSTOCKQTY,A2.PURQTY)*1 AS PURQTY,
         |	A3.INSTOCKQTY
         |FROM A2
         |LEFT JOIN A3 ON A3.FSBILLID = A2.FID AND A3.FSID = A2.FENTRYID
         |UNION ALL
         |SELECT
         | 	A2.FBILLNO AS PURNO,
         | 	A2.FDOCUMENTSTATUS AS PURSTATUS,
         | 	A2.SUPPLIERNAME,
         | 	A2.PURDEPTNMEA,
         | 	A2.PURCHASERNAME,
         | 	A2.FCREATEDATE AS PURCREADATE,
         | 	A2.FAPPROVEDATE AS PURAPPDATE,
         |	A2.FSBILLID,
         |	A2.FSID,
         |	A2.FID,
         |	A2.FENTRYID,
         | 	NULL AS INCREADATE,
         | 	NULL AS INAPPDATE,
         | 	NULL AS INSTOCKNO,
         | 	NULL AS INSTOCKFLOT,
         | 	NULL AS FMATERIALID,
         | 	NULL AS INSTOCK,
         |	A3.INSTOCKQTY-SUM(A2.PURQTY) AS PURQTY,
         |	NULL AS INSTOCKQTY
         |FROM A2
         |LEFT JOIN A3 ON A3.FSBILLID = A2.FID AND A3.FSID = A2.FENTRYID
         |GROUP BY A2.FBILLNO,
         |	A2.FCREATEDATE,
         |	A2.FAPPROVEDATE ,
         |	A2.FDOCUMENTSTATUS,
         |	A2.SUPPLIERNAME ,
         |	A2.PURDEPTNMEA,
         |	A2.PURCHASERNAME,
         |	A2.FSBILLID,
         |	A2.FSID,
         |	A2.FID,
         |	A2.FENTRYID,
         |	A3.INSTOCKQTY
         |HAVING A3.INSTOCKQTY-SUM(A2.PURQTY) > 0
         |""".stripMargin).createOrReplaceTempView("A23")

    spark.sql(
      s"""
         |SELECT
         | 	A1.FBILLNO AS REQNO,
         | 	A1.FDOCUMENTSTATUS AS REQSTATUS,
         | 	A1.REQORGNAME,
         | 	A1.FAPPLICATIONDATE AS REQAPPLIDATE,
         | 	A1.F_PROJECTNO AS REQPRONO,
         | 	A1.SALEORGNAME,
         | 	A1.SALER,
         | 	A1.MATERNO,
         | 	A1.MATERNAME,
         | 	A1.BRANDNAME,
         | 	A1.SPECIFICATION,
         | 	A1.KHLB AS TESTTYPE,
         | 	A1.FCREATEDATE AS REQCREADATE,
         | 	A1.FAPPROVEDATE AS REQAPPDATE,
         | 	A1.PROJECTNAME,
         | 	A1.FID,
         | 	A1.FENTRYID,
         | 	A23.PURNO,
         | 	A23.PURSTATUS,
         | 	A23.SUPPLIERNAME,
         | 	A23.PURDEPTNMEA,
         | 	A23.PURCHASERNAME,
         | 	A23.PURCREADATE,
         | 	A23.PURAPPDATE,
         |	A1.FSBILLID,
         |	A1.FSID,
         | 	A23.INCREADATE,
         | 	A23.INAPPDATE,
         | 	A23.INSTOCKNO,
         | 	A23.INSTOCKFLOT,
         | 	A23.FMATERIALID,
         | 	A23.INSTOCK,
         |	COALESCE(A23.PURQTY,A1.REQQTY)*1 AS REQQTY,
         |	A23.PURQTY,
         |	A23.INSTOCKQTY
         |FROM A1
         |LEFT JOIN A23 ON A23.FSBILLID = A1.FID AND A23.FSID = A1.FENTRYID
         |UNION ALL
         | SELECT
         | 	A1.FBILLNO AS REQNO,
         | 	A1.FDOCUMENTSTATUS AS REQSTATUS,
         | 	A1.REQORGNAME,
         | 	A1.FAPPLICATIONDATE AS REQAPPLIDATE,
         | 	A1.F_PROJECTNO AS REQPRONO,
         | 	A1.SALEORGNAME,
         | 	A1.SALER,
         | 	A1.MATERNO,
         | 	A1.MATERNAME,
         | 	A1.BRANDNAME,
         | 	A1.SPECIFICATION,
         | 	A1.KHLB AS TESTTYPE,
         | 	A1.FCREATEDATE AS REQCREADATE,
         | 	A1.FAPPROVEDATE AS REQAPPDATE,
         | 	A1.PROJECTNAME,
         | 	A1.FID,
         | 	A1.FENTRYID,
         | 	NULL AS PURNO,
         | 	NULL AS PURSTATUS,
         | 	NULL AS SUPPLIERNAME,
         | 	NULL AS PURDEPTNMEA,
         | 	NULL AS PURCHASERNAME,
         | 	NULL AS PURCREADATE,
         | 	NULL AS PURAPPDATE,
         |	A1.FSBILLID,
         |	A1.FSID,
         | 	NULL AS INCREADATE,
         | 	NULL AS INAPPDATE,
         | 	NULL AS INSTOCKNO,
         | 	NULL AS INSTOCKFLOT,
         | 	NULL AS FMATERIALID,
         | 	NULL AS INSTOCK,
         |	A1.REQQTY - SUM(A23.PURQTY) AS REQQTY,
         |	NULL AS PURQTY,
         |	NULL AS INSTOCKQTY
         |FROM A1
         |LEFT JOIN A23 ON A23.FSBILLID = A1.FID AND A23.FSID = A1.FENTRYID
         |GROUP BY A1.FBILLNO ,
         | 	A1.FDOCUMENTSTATUS ,
         | 	A1.REQORGNAME,
         | 	A1.FAPPLICATIONDATE,
         | 	A1.F_PROJECTNO ,
         | 	A1.SALEORGNAME,
         | 	A1.SALER,
         | 	A1.MATERNO,
         | 	A1.MATERNAME,
         | 	A1.BRANDNAME,
         | 	A1.SPECIFICATION,
         | 	A1.KHLB,
         | 	A1.FCREATEDATE ,
         | 	A1.FAPPROVEDATE,
         | 	A1.PROJECTNAME,
         | 	A1.REQQTY,
         | 	A1.FID,
         | 	A1.FENTRYID,
         |  A1.FSBILLID,
         |	A1.FSID
         | HAVING A1.REQQTY - SUM(A23.PURQTY) > 0
         |""".stripMargin).createOrReplaceTempView("A123")
    /**
     * 出库通知单获取批号
     *
     */
    spark.sql(
      s"""
         |SELECT OED.FBILLNO,
         |	OED.FCREATEDATE,
         |	OED.FAPPROVEDATE,
         |	OEDE.FMATERIALID,
         |	DL.FNAME FLOTNAME,
         |	SUM(OEDE.FQTY) OVER(PARTITION BY DL.FNAME,OEDE.FMATERIALID) NOTICEQTY,
         |	ROW_NUMBER() OVER(PARTITION BY DL.FNAME,OEDE.FMATERIALID ORDER BY COALESCE(OED.FAPPROVEDATE,OED.FCREATEDATE) DESC) AS RN
         |FROM ${TableName.ODS_ERP_DELIVERYNOTICE} OED
         |LEFT JOIN ${TableName.ODS_ERP_DELIVERYNOTICEENTRY} OEDE ON OED.FID = OEDE.FID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} DL ON OEDE.FLOT = DL.FLOTID
         |WHERE COALESCE(OEDE.FLOT,'') != ''
         |""".stripMargin).createOrReplaceTempView("A4")

    spark.sql(
      s"""
         |SELECT FBILLNO,
         |	FCREATEDATE,
         |	FAPPROVEDATE,
         |	FMATERIALID,
         |	FLOTNAME,
         |	IF(NOTICEQTY='',NULL, NOTICEQTY) AS NOTICEQTY
         |FROM A4
         |WHERE RN = 1
         |""".stripMargin).createOrReplaceTempView("A5")

    /**
     * 出库单
     */
    spark.sql(
      s"""
        |SELECT
        |  	OEO.FBILLNO,
        |	OEO.FCREATEDATE,
        |	OEO.FAPPROVEDATE,
        |	OEOE.FMATERIALID,
        |	DL.FNAME AS FLOTNAME,
        |	SUM(OEOE.FREALQTY) OVER(PARTITION BY DL.FNAME,OEOE.FMATERIALID) AS OUTSTOCKQTY,
        |	ROW_NUMBER() OVER(PARTITION BY DL.FNAME,OEOE.FMATERIALID ORDER BY COALESCE(OEO.FAPPROVEDATE,OEO.FCREATEDATE) DESC) AS RN
        |FROM ${TableName.ODS_ERP_OUTSTOCK} OEO
        |LEFT JOIN ${TableName.ODS_ERP_OUTSTOCKFIN} OEOF ON OEO.FID = OEOF.FID
        |LEFT JOIN ${TableName.ODS_ERP_OUTSTOCKENTRY} OEOE ON OEO.FID = OEOE.FID
        |LEFT JOIN ${TableName.ODS_ERP_OUTSTOCKENTRY_LK} OEOL ON OEOE.FENTRYID = OEOL.FENTRYID
        |LEFT JOIN ${TableName.DIM_LOTMASTER} DL ON OEOE.FLOT = DL.FLOTID
        |WHERE COALESCE(OEOE.FLOT,'') != '' AND OEOF.FISGENFORIOS = '0'
        |""".stripMargin).createOrReplaceTempView("A6")

    spark.sql(
      s"""
         |SELECT
         | 	FBILLNO,
         |	FCREATEDATE,
         |	FAPPROVEDATE,
         |	FMATERIALID,
         |	FLOTNAME,
         |	IF(OUTSTOCKQTY=='',NULL,OUTSTOCKQTY) AS OUTSTOCKQTY
         |FROM A6
         |WHERE RN = 1
         |""".stripMargin).createOrReplaceTempView("A7")

    /**
     * 销售订单
     */
    spark.sql(
      s"""
         |SELECT
         |	DSO.FBILLNO,
         |	DSO.FCREATEDATE,
         |	DSO.FAPPROVEDATE,
         |	DSO.FID,
         |	DSO.FENTRYID,
         |	DSO.FMATERIALID,
         |	DP.FCUSTOMERORDERID,
         |	IF(FQTY='',NULL,FQTY) AS SALEQTY
         |FROM ${TableName.DWD_SAL_ORDER} DSO
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON DSO.FPROJECTBASIC = DP.FID
         |WHERE FBILLNO <> ''
         |ORDER BY DSO.FCREATEDATE DESC
         |""".stripMargin).createOrReplaceTempView("A8")


    spark.sql(
      s"""
         |SELECT
         | 	A123.REQNO,
         | 	A123.REQSTATUS,
         | 	A123.REQORGNAME,
         | 	A123.REQAPPLIDATE,
         | 	A123.REQPRONO,
         | 	A123.SALEORGNAME,
         | 	A123.SALER,
         | 	A123.MATERNO,
         | 	A123.MATERNAME,
         | 	A123.BRANDNAME,
         | 	A123.SPECIFICATION,
         | 	A123.TESTTYPE,
         | 	A123.REQCREADATE,
         | 	A123.REQAPPDATE,
         | 	A123.PROJECTNAME,
         | 	A123.FID,
         | 	A123.FENTRYID,
         | 	A123.PURNO,
         | 	A123.PURSTATUS,
         | 	A123.SUPPLIERNAME,
         | 	A123.PURDEPTNMEA,
         | 	A123.PURCHASERNAME,
         | 	A123.PURCREADATE,
         | 	A123.PURAPPDATE,
         |	A123.FSBILLID,
         |	A123.FSID,
         | 	A123.INCREADATE,
         | 	A123.INAPPDATE,
         | 	A123.INSTOCKNO,
         | 	A123.INSTOCKFLOT,
         | 	A123.FMATERIALID,
         | 	A123.INSTOCK,
         |	A8.FBILLNO AS SALENO,
         |	A8.FCREATEDATE AS SALECREADATE,
         | 	A8.FAPPROVEDATE AS SALEAPPDATE,
         | 	A8.FCUSTOMERORDERID,
         | 	A8.FENTRYID,
         | 	A8.FCUSTOMERORDERID,
         | 	COALESCE(A123.REQQTY,A8.SALEQTY)*1 AS SALEQTY,
         | 	A123.REQQTY,
         |	A123.PURQTY,
         |	A123.INSTOCKQTY
         | FROM A123
         | LEFT JOIN A8 ON A8.FID = A123.FSBILLID AND A8.FENTRYID = A123.FSID
         | UNION ALL
         |   SELECT
         | 		NULL AS REQNO,
         | 	NULL AS REQSTATUS,
         | 	NULL AS REQORGNAME,
         | 	NULL AS REQAPPLIDATE,
         | 	NULL AS REQPRONO,
         | 	NULL AS SALEORGNAME,
         | 	NULL AS SALER,
         | 	NULL AS MATERNO,
         | 	NULL AS MATERNAME,
         | 	NULL AS BRANDNAME,
         | 	NULL AS SPECIFICATION,
         | 	NULL AS TESTTYPE,
         | 	NULL AS REQCREADATE,
         | 	NULL AS REQAPPDATE,
         | 	NULL AS PROJECTNAME,
         | 	NULL AS FID,
         | 	NULL AS FENTRYID,
         | 	NULL AS PURNO,
         | 	NULL AS PURSTATUS,
         | 	NULL AS SUPPLIERNAME,
         | 	NULL AS PURDEPTNMEA,
         | 	NULL AS PURCHASERNAME,
         | 	NULL AS PURCREADATE,
         | 	NULL AS PURAPPDATE,
         |	NULL AS FSBILLID,
         |	NULL AS FSID,
         | 	NULL AS INCREADATE,
         | 	NULL AS INAPPDATE,
         | 	NULL AS INSTOCKNO,
         | 	NULL AS INSTOCKFLOT,
         | 	NULL AS FMATERIALID,
         | 	NULL AS INSTOCK,
         |	A8.FBILLNO AS SALENO,
         |	A8.FCREATEDATE AS SALECREADATE,
         | 	A8.FAPPROVEDATE AS SALEAPPDATE,
         | 	A8.FCUSTOMERORDERID,
         | 	A8.FENTRYID,
         | 	A8.FCUSTOMERORDERID,
         | 	A8.SALEQTY - SUM(A123.REQQTY) AS SALEQTY,
         | 	NULL AS REQQTY,
         |	NULL AS PURQTY,
         |	NULL AS INSTOCKQTY
         | FROM A123
         | LEFT JOIN A8 ON A8.FID = A123.FSBILLID AND A8.FENTRYID = A123.FSID
         | GROUP BY
         |	A8.FBILLNO,
         |	A8.FCREATEDATE,
         | 	A8.FAPPROVEDATE,
         | 	A8.FCUSTOMERORDERID,
         | 	A8.SALEQTY,
         | 	A8.FENTRYID,
         | 	A8.FCUSTOMERORDERID
         | HAVING A8.SALEQTY - SUM(A123.REQQTY) > 0
         |""".stripMargin).createOrReplaceTempView("A1238")

    /**
     * a1 采购申请单  a2 采购订单  a3 采购入库单 a5 出库通知单   a7 采购出库单  a8销售订单
     */

    val res = spark.sql(
      s"""
         |SELECT
         |	A1238.REQNO,
         | 	A1238.REQSTATUS,
         | 	A1238.REQORGNAME,
         | 	A1238.REQAPPLIDATE,
         | 	A1238.REQPRONO,
         | 	A1238.SALEORGNAME,
         | 	A1238.SALER,
         | 	A1238.SALENO,
         | 	A1238.PURNO,
         | 	A1238.PURSTATUS,
         | 	A1238.SUPPLIERNAME,
         | 	A1238.PURDEPTNMEA,
         | 	A1238.PURCHASERNAME,
         | 	A1238.MATERNO,
         | 	A1238.MATERNAME,
         | 	A1238.BRANDNAME,
         | 	A1238.SPECIFICATION,
         | 	A1238.TESTTYPE,
         | 	A1238.SALECREADATE,
         | 	A1238.SALEAPPDATE,
         | 	A1238.REQCREADATE,
         | 	A1238.REQAPPDATE,
         | 	A1238.PURCREADATE,
         | 	A1238.PURAPPDATE,
         | 	A1238.INCREADATE,
         | 	A1238.INAPPDATE,
         | 	A5.FCREATEDATE AS SENDCREADATE,
         | 	A5.FAPPROVEDATE AS SENDAPPDATE,
         | 	A7.FCREATEDATE AS OUTCREADATE,
         | 	A7.FAPPROVEDATE AS OUTAPPDATE,
         | 	(UNIX_TIMESTAMP(CAST(A1238.SALEAPPDATE AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(A1238.SALECREADATE AS TIMESTAMP))) / 3600 / 24.0 AS SALFORDATE,
         | 	(UNIX_TIMESTAMP(CAST(A1238.REQAPPDATE AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(A1238.REQCREADATE AS TIMESTAMP))) / 3600 / 24.0 AS REQFORDATE,
         | 	(UNIX_TIMESTAMP(CAST(A1238.PURAPPDATE AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(A1238.REQAPPDATE AS TIMESTAMP))) / 3600 / 24.0 AS POOFORDATE,
         | 	A1238.REQQTY,
         | 	A1238.PURQTY,
         | 	A1238.INSTOCKNO,
         | 	A1238.INSTOCKFLOT,
         | 	A1238.INSTOCK,
         | 	A1238.INSTOCKQTY,
         |	A5.FBILLNO AS NOTICENO,
         | 	A5.NOTICEQTY AS NOTICEQTY,
         | 	A7.FBILLNO AS OUTSTOCKNO,
         | 	A1238.SALEQTY,
         | 	A7.OUTSTOCKQTY AS OUTSTOCKQTY,
         | 	A1238.PROJECTNAME,
         | 	A1238.FCUSTOMERORDERID,
         | 	(UNIX_TIMESTAMP(CAST(A7.FAPPROVEDATE AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(A1238.INAPPDATE AS TIMESTAMP))) / 3600 / 24.0 AS INTOOUTFORDATE
         | FROM A1238
         | LEFT JOIN A5 ON A5.FLOTNAME = A1238.INSTOCKFLOT AND A5.FMATERIALID = A1238.FMATERIALID
         | LEFT JOIN A7 ON A7.FLOTNAME = A1238.INSTOCKFLOT AND A7.FMATERIALID = A1238.FMATERIALID
         |""".stripMargin)
    println("result=" + res.count())
    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_pur_taketime"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res.write.mode("overwrite").jdbc(url, table, props)

  }

  /**
   *采购过程--金额取数
   * @param spark
   */
  def purAmount(spark: SparkSession) = {
    //已开票金额
    spark.sql(
      s"""
         |SELECT	--已开票金额
         |	substring(oer.fdate,1,10) fdate,
         |	oere.F_PAEZ_BASE purdept,
         |	oere.F_PAEZ_BASE1 purperson,
         |	oere.fmaterialid,
         |	SUM(oere.FPRICEQTY * OERE.FTAXPRICE) AS FAMOUNT
         |FROM ${TableName.ODS_ERP_RECEIVABLE} oer
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} oere ON OER.fid = oere.fid
         |WHERE oer.FSETTLEORGID = 1 and FDOCUMENTSTATUS = 'C'
         |GROUP BY
         |oere.F_PAEZ_BASE ,
         |	oere.F_PAEZ_BASE1 ,
         |	oere.fmaterialid,
         |	substring(oer.fdate,1,10)
         |""".stripMargin).createOrReplaceTempView("a1")
    //--未开票金额
    spark.sql(
      s"""
         |SELECT fdate,PURDEPT,PURPERSON,fmaterialid,SUM(FAMOUNT) FAMOUNT FROM (
         |select 	--未开票金额
         |substring(oea.fdate,1,10) fdate,
         |oead.F_PXDF_BASE2 purdept,
         |oead.F_PXDF_BASE1 purperson,
         |oead.fmaterialid,
         |oead.F_PXDF_QTY * oead.FTAXPRICE AS FAMOUNT
         |from ${TableName.ODS_ERP_ARSETTLEMENT} oea
         |left join ${TableName.ODS_ERP_ARSETTLEMENTDETAIL} oead on oea.fid = oead.fid
         |where oea.FACCTORGID = 1
         |UNION ALL
         |select --未开票金额
         |substring(oeo.fdate,1,10) fdate,
         |oeoe.F_PAEZ_BASE2 purdept,
         |oeoe.F_PAEZ_BASE1 pyrperson,
         |oeoe.fmaterialid,
         |oeor.FARNOTJOINQTY*oeof.FTAXPRICE AS FAOMUNT
         |from ${TableName.ODS_ERP_OUTSTOCK} oeo
         |left join ${TableName.ODS_ERP_OUTSTOCKENTRY} oeoe on oeo.fid = oeoe.fid
         |left join ${TableName.ODS_ERP_OUTSTOCKENTRY_F} oeof on oeoe.fentryid = oeof.fentryid
         |left join ${TableName.ODS_ERP_OUTSTOCKENTRY_R} oeor on oeoe.fentryid = oeor.fentryid
         |where oeo.FSTOCKORGID = 1
         |) WEI
         |GROUP BY fdate,PURDEPT,PURPERSON,fmaterialid
         |""".stripMargin).createOrReplaceTempView("a2")

    //在执行金额
    spark.sql(
      s"""
         |SELECT --在执行金额
         |substring(oep.fdate,1,10) fdate,
         |oep.FPURCHASEDEPTID purdept,
         |oep.FPURCHASERID purperson,
         |oepe.fmaterialid,
         |sum(oepe.FQTY*oepf.FTAXPRICE) AS FAMOUNT
         |FROM ${TableName.ODS_ERP_POORDER} oep
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY} oepe on oep.fid =oepe.fid
         |left join ${TableName.ODS_ERP_POORDERENTRY_F} oepf on oepe.fentryid = oepf.fentryid
         |where FPURCHASEORGID = 1 and oepe.FMRPCLOSESTATUS = 'A'
         |group by
         |substring(oep.fdate,1,10),
         |oep.FPURCHASEDEPTID,
         |oep.FPURCHASERID,
         |oepe.fmaterialid
         |""".stripMargin).createOrReplaceTempView("a3")

    spark.sql(
      s"""
         |SELECT
         |substring(oei.fdate,1,10) fdate,
         |oei.FPURCHASEDEPTID purdept,
         |oei.FPURCHASERID purperson,
         |oeie.fmaterialid,
         |oeif.F_PAEZ_AMOUNT famount
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie on oei.fid = oeie.fid
         |left join ${TableName.ODS_ERP_INSTOCKENTRY_F} oeif on oeie.fentryid = oeif.fentryid
         |left join ${TableName.DIM_STOCK} ds on oeie.FSTOCKID = ds.FSTOCKID
         |where oeie.FGIVEAWAY = 0 and ds.fname not like '%样品%' and oei.FSTOCKORGID = 1
         |""".stripMargin).createOrReplaceTempView("a4")

    spark.sql(
      s"""
         |SELECT
         |	COALESCE (if(a1.fdate='',null,a1.fdate),if(a2.fdate='',null,a2.fdate),if(a3.fdate='',null,a3.fdate),if(a4.fdate='',null,a4.fdate)) as fdate,
         |	COALESCE (if(a1.purdept='',null,a1.purdept),if(a2.purdept='',null,a2.purdept),if(a3.purdept='',null,a3.purdept),if(a4.purdept='',null,a4.purdept)) as purdept,
         |	COALESCE (if(a1.purperson='',null,a1.purperson),if(a2.purperson='',null,a2.purperson),if(a3.purperson='',null,a3.purperson),if(a4.purperson='',null,a4.purperson)) as purperson,
         |	COALESCE (if(a1.fmaterialid='',null,a1.fmaterialid),if(a2.fmaterialid='',null,a2.fmaterialid),if(a3.fmaterialid='',null,a3.fmaterialid),if(a4.fmaterialid='',null,a4.fmaterialid)) as fmaterialid,
         |	cast(nvl(a1.famount,0) as decimal(18,4)) ykpamount,
         |	cast(nvl(a2.famount,0) as decimal(18,4)) wkpamount,
         |	cast(nvl(a3.famount,0) as decimal(18,4)) zzxamount,
         |	cast(nvl(a4.famount,0) as decimal(18,4)) wdpamount
         |from a1
         |full join a2 on a1.purdept = a2.purdept and a1.purperson = a2.purperson and a1.fmaterialid = a2.fmaterialid and a1.fdate = a2.fdate
         |full join a3 on a2.purdept = a3.purdept and a2.purperson = a3.purperson and a2.fmaterialid = a3.fmaterialid and a2.fdate = a3.fdate
         |full join a4 on a4.purdept = a3.purdept and a4.purperson = a3.purperson and a4.fmaterialid = a3.fmaterialid and a4.fdate = a3.fdate
         |""".stripMargin).createOrReplaceTempView("a5")


    val res2 = spark.sql(
      s"""
         |SELECT A5.FDATE,
         |	dd.fname purdeptname,
         |	db.fname buyername,
         |	dm.fnumber materno,
         |	dm.fname matername,
         |	dm.f_paez_text brandname,
         |	dm.fspecification specification,
         |	a5.ykpamount ykpamount,
         |	a5.wkpamount wkpamount,
         |	a5.zzxamount zzxamount,
         |	a5.wdpamount wdpamount,
         |	dc.fname assesstype
         |FROM a5
         |LEFT JOIN ${TableName.DIM_MATERIAL} dm ON a5.fmaterialid = dm.fmaterialid
         |LEFT JOIN ${TableName.DIM_CUST100501} DC ON dm.f_khr = DC.FID
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} dd ON dd.fdeptid  = a5.PURDEPT
         |LEFT JOIN ${TableName.DIM_BUYER} db ON a5.PURPERSON = db.fid
         |WHERE a5.ykpamount <> 0 or a5.wkpamount <> 0 or a5.zzxamount <> 0 or a5.wdpamount <> 0
         |""".stripMargin)
    println("num==============" + res2.count())
    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_pur_amount"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res2.write.mode("overwrite").jdbc(url, table, props)
  }

  def runRes3(spark: SparkSession): Unit = {
    spark.sql(
      """
        |SELECT
        | namee
        |FROM
        |keetle_test.tes_tmp
        |group by namee
        |WHERE cast(idf as Double) <> '' or namee <> '' or cast(age as Double) <> '' or gender <> ''
        |""".stripMargin).show()
    println("=====================")
    spark.sql(
      """
        |SELECT
        | *
        |FROM
        |keetle_test.tes_tmp WHERE cast(idf as Double) <> 0 or namee <> 0 or cast(age as Double) <> 0 or gender <> 0
        |""".stripMargin).show()
  }
  def runRes4(spark: SparkSession): Unit = {
    spark.sql("""select oep.fbillno,
                |		oep.fcreatedate,
                |		oep.fapprovedate,
                |		oepe.fmaterialid,
                |		oepr.fsrcbillno,
                |		case when oep.fdocumentstatus = 'Z' then '暂存'
                |	 		 when oep.fdocumentstatus = 'A' then '创建'
                |	 		 when oep.fdocumentstatus = 'B' then '审核中'
                |	 		 when oep.fdocumentstatus = 'C' then '已审核'
                |	 		 when oep.fdocumentstatus = 'D' then '重新审核'
                |	 		 else oep.fdocumentstatus end as fdocumentstatus,
                |		ds.fname suppliername ,
                |		dd.fname purdeptnmea,
                |		db.fname purchasername,
                |		oepl.fsbillid,
                |		oepl.fsid,
                |		oep.fid,
                |		oepe.fentryid
                |	from ods_xhgj.ods_erp_poorder oep
                |	left join ods_xhgj.ods_erp_poorderentry oepe on oep.fid = oepe.fid
                |	left join ods_xhgj.ods_erp_poorderentry_r oepr on oepe.fentryid = oepr.fentryid
                |	left join ods_xhgj.ods_erp_poorderentry_lk oepl on oepl.fentryid = oepe.fentryid
                |	LEFT JOIN dw_xhgj.dim_department dd on oep.fpurchasedeptid = dd.fdeptid
                |	LEFT JOIN dw_xhgj.dim_buyer db on oep.fpurchaserid = db.fid
                |	LEFT JOIN dw_xhgj.dim_supplier ds ON oep.fsupplierid = ds.fsupplierid
                |	 WHERE oep.fbillno = 'WJ00347483' and oep.FBILLNO is not null and oepr.FSRCBILLNO is not null""".stripMargin).show(100)

  }

  /**
   * 采购过程--退货单
   * @param spark
   */
  def RerurnStock(spark: SparkSession): Unit = {
    val res = spark.sql(
      s"""
         |SELECT OER.FDATE,
         |	DB.FNAME BUYERNAME,
         |	OER.F_PAEZ_TEXT4 ORGNAME,
         |	DS.FNAME SALERNAME,
         |	OER.FBILLNO ,
         |	OERE.FMATERIALID,
         |	DM.FNAME MATERIALNAME
         |FROM ${TableName.ODS_ERP_RETURNSTOCK} OER
         |LEFT JOIN ${TableName.ODS_ERP_RETURNSTOCKFIN} OERF ON OER.FID = OERF.FID
         |LEFT JOIN ${TableName.ODS_ERP_RETURNSTOCKENTRY} OERE ON OER.FID  = OERE.FID
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OER.FSALESMANID = DS.FID
         |LEFT JOIN ${TableName.DIM_MATERIAL} DM ON OERE.FMATERIALID = DM.FMATERIALID
         |LEFT JOIN ${TableName.ODS_ERP_MATERIALPURCHASE} OEM ON DM.FMATERIALID = OEM.FMATERIALID
         |LEFT JOIN ${TableName.DIM_BUYER} DB ON OEM.FPURCHASERID = DB.FID
         |WHERE FSTOCKORGID = '1' AND OER.FDOCUMENTSTATUS = 'C' AND OERF.FISGENFORIOS = '0'
         |""".stripMargin)
    println("num==============" + res.count())
    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_pur_returnstock"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res.write.mode("overwrite").jdbc(url, table, props)

  }

  /**
   * 采购过程--工作量
   * @param spark
   */
  def WorkLoad(spark: SparkSession)={
    spark.sql(
      s"""
         |SELECT SUBSTRING(OER.FAPPROVEDATE,1,10) AS FDATE,
         |	DB.FNAME AS BUYERNAME,
         |	COUNT(*) NUM
         |FROM ${TableName.ODS_ERP_REQUISITION} OER
         |LEFT JOIN ${TableName.ODS_ERP_REQENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.ODS_ERP_REQENTRY_S} OERS ON OERE.FENTRYID = OERS.FENTRYID
         |LEFT JOIN ${TableName.DIM_BUYER} DB ON OERS.FPURCHASERID = DB.FID
         |WHERE (OER.FAPPLICATIONORGID = '1' or OER.FAPPLICATIONORGID = '481351') AND OER.FDOCUMENTSTATUS = 'C'
         |GROUP BY SUBSTRING(OER.FAPPROVEDATE,1,10),
         |	DB.FNAME
         |""".stripMargin).createOrReplaceTempView("A1")

    spark.sql(
      s"""
         |SELECT SUBSTRING(OEP.FAPPROVEDATE,1,10) AS FDATE,
         |	DU.FNAME AS BUYERNAME,
         |	COUNT(*) AS NUM
         |FROM ${TableName.ODS_ERP_POORDER} OEP
         |LEFT JOIN ${TableName.DIM_USER} DU ON OEP.FCREATORID = DU.FUSERID
         |WHERE (OEP.FPURCHASEORGID = '1' OR OEP.FPURCHASEORGID = '481351') AND OEP.FDOCUMENTSTATUS = 'C'
         |GROUP BY SUBSTRING(OEP.FAPPROVEDATE,1,10) ,
         |	DU.FNAME
         |""".stripMargin).createOrReplaceTempView("A2")

    spark.sql(
      s"""
         |SELECT SUBSTRING(OEP.FDATE,1,10) AS FDATE,
         |	DU.FNAME AS BUYERNAME,
         |	COUNT(*) AS NUM
         |FROM ${TableName.ODS_ERP_PAYBILL} OEP
         |LEFT JOIN ${TableName.DIM_USER} DU ON OEP.FCREATORID = DU.FUSERID
         |WHERE (OEP.FPAYORGID = 1 OR OEP.FPAYORGID = '481351') AND OEP.FDOCUMENTSTATUS = 'C'
         |GROUP BY SUBSTRING(OEP.FDATE,1,10) ,
         |	DU.FNAME
         |""".stripMargin).createOrReplaceTempView("A3")

    spark.sql(
      s"""
         |SELECT SUBSTRING(OEP.FDATE,1,10) AS FDATE,
         |	DB.FNAME AS BUYERNAME,
         |	DS.FNAME AS PAYTYPENAME,
         |	SUM(OEPE.FREALPAYAMOUNT) AS KPAMOUNT
         |FROM ${TableName.ODS_ERP_PAYBILL} OEP
         |LEFT JOIN ${TableName.ODS_ERP_PAYBILLENTRY} OEPE ON OEP.FID = OEPE.FID
         |LEFT JOIN ${TableName.DIM_BUYER} DB ON OEP.FPURCHASERID = DB.FID
         |LEFT JOIN ${TableName.DIM_SETTLETYPE} DS ON OEPE.FSETTLETYPEID = DS.FID
         |WHERE (OEP.FPAYORGID = 1 OR OEP.FPAYORGID = '481351') AND OEP.FDOCUMENTSTATUS = 'C'
         |GROUP BY SUBSTRING(OEP.FDATE,1,10) ,
         |	DB.FNAME ,
         |	DS.FNAME
         |""".stripMargin).createOrReplaceTempView("A4")

    val res = spark.sql(
      s"""
         |SELECT
         |	COALESCE (A1.FDATE,A2.FDATE) AS FAPPROVEDATE,
         |  COALESCE (A3.FDATE,A4.FDATE) AS FDATE,
         |	COALESCE (A1.BUYERNAME,A2.BUYERNAME,A3.BUYERNAME,A4.BUYERNAME) AS BUYERNAME,
         |	A4.PAYTYPENAME,
         |	A1.NUM AS REQNUM,
         |	A2.NUM AS PURNUM,
         |	A3.NUM AS FKDNUM,
         |	A4.KPAMOUNT
         |FROM A1
         |FULL JOIN A2 ON A1.FDATE = A2.FDATE AND A1.BUYERNAME = A2.BUYERNAME
         |FULL JOIN A3 ON A1.FDATE = A3.FDATE AND A1.BUYERNAME = A3.BUYERNAME
         |FULL JOIN A4 ON A1.FDATE = A4.FDATE AND A1.BUYERNAME = A4.BUYERNAME
         |""".stripMargin)
    println("num==============" + res.count())

    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_pur_workload"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res.write.mode("overwrite").jdbc(url, table, props)
  }
}

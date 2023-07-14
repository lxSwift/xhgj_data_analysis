package com.xhgj.bigdata.otherProject

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/7/6 9:38
 * @PackageName:com.xhgj.bigdata.otherProject
 * @ClassName: ApplicationToOrderTime
 * @Description: TODO
 * @Version 1.0
 */
object ApplicationToOrderTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job ApplicationToOrderTime.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark:SparkSession)={
    /**
     * 1.取采购订单列表，取采购组织为“万聚国际（杭州）供应链有限公司”；
     * 2.以采购订单列表为主表，关联对应的采购申请单和销售订单列表。
     * 3.过滤源单单号为空的采购订单
     * 相差日期: 采购订单创建日期减采购申请单审核日期
     */

    spark.sql(
      s"""
         |SELECT
         |  OEP.FBILLNO,
         |	OEP.FCREATEDATE,
         |	OEP.FAPPROVEDATE,
         |	C.FNUMBER FMATERIALID,
         | C.F_PAEZ_BASE,
         | C.fname,
         | C.FSPECIFICATION,
         |	OEPR.FSRCBILLNO,
         |	CASE WHEN OEP.FDOCUMENTSTATUS = 'Z' THEN '暂存'
         | 		 WHEN OEP.FDOCUMENTSTATUS = 'A' THEN '创建'
         | 		 WHEN OEP.FDOCUMENTSTATUS = 'B' THEN '审核中'
         | 		 WHEN OEP.FDOCUMENTSTATUS = 'C' THEN '已审核'
         | 		 WHEN OEP.FDOCUMENTSTATUS = 'D' THEN '重新审核'
         | 		 ELSE OEP.FDOCUMENTSTATUS END AS FDOCUMENTSTATUS,
         |	OEPL.FSBILLID,
         | OEP.FSUPPLIERID,
         | OEP.FPROVIDERCONTACTID,
         |	OEPL.FSID,
         |	OEP.FID,
         |	OEPE.FENTRYID,
         |	OEPE.FQTY AS PURQTY,
         | OEPE.FUNITID,
         | OEPE.F_PAEZ_BASE2,
         | OEP.FCREATORID,
         | OEP.FPURCHASEDEPTID,
         | OEPE.F_PAEZ_BASE1,
         | OEPE.F_PAEZ_TEXT
         |FROM ${TableName.ODS_ERP_POORDER} OEP
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY} OEPE ON OEP.FID = OEPE.FID
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY_R} OEPR ON OEPE.FENTRYID = OEPR.FENTRYID
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY_LK} OEPL ON OEPL.FENTRYID = OEPE.FENTRYID
         |LEFT JOIN ${TableName.DIM_MATERIAL} C ON OEPE.FMATERIALID = C.FMATERIALID
         |WHERE OEP.FPURCHASEORGID = '1' and OEPR.FSRCBILLNO != '' AND OEPR.FSRCBILLNO IS NOT NULL
         |""".stripMargin).createOrReplaceTempView("salorder")

    spark.sql(
      s"""
         |SELECT
         |  oer.fbillno,
         | 	oer.fapprovedate,
         | 	oer.fid,
         | 	oere.fentryid
         |FROM
         |  ${TableName.ODS_ERP_REQUISITION} oer
         |left join ${TableName.ODS_ERP_REQENTRY} oere on oer.fid = oere.fid
         |""".stripMargin).createOrReplaceTempView("req")
    //销售订单相关去重
    spark.sql(
      s"""
         |SELECT
         |OES.FBILLNO,
         |C.FNUMBER,
         |OES.F_PXDF_TEXT,
         |OES.F_PXDF_TEXT1
         |FROM ${TableName.ODS_ERP_SALORDER} OES
         |LEFT JOIN ${TableName.ODS_ERP_SALORDERENTRY} OESE ON OES.FID = OESE.FID
         |LEFT JOIN ${TableName.DIM_MATERIAL} C ON OESE.FMATERIALID = C.FMATERIALID
         |""".stripMargin).createOrReplaceTempView("SALORDER_L")



    val result = spark.sql(
      s"""
         |SELECT
         |  b.FBILLNO REQBILLNO,--采购申请单编号
         |  b.FAPPROVEDATE REQAPPROVEDATE,--采购申请单审核日期
         |  sal.FCREATEDATE FCREATEDATE,--采购订单创建日期
         |  sal.FAPPROVEDATE,--采购订单审核日期
         |  sal.FBILLNO,--单据编号
         |  SAL.FDOCUMENTSTATUS,--单据状态
         |  SUP.FNAME SUPPLIER,--供应商
         |  SUPC.FCONTACT,--供货方联系人
         |  SUPC.FMOBILE,--手机
         |  SAL.FMATERIALID,--物料编码
         |  ENT.FNAME PAEZNAME,--品牌
         |  SAL.FNAME MATERIALNAME,--物料名称
         |  SAL.FSPECIFICATION,--规格型号
         |  SAL.PURQTY,--采购数量
         |  UNIT.fname UNITNAME,--采购单位
         |  STOCK.FNAME STOCKNAME,--仓库
         |  USER.FNAME CREATOR,--创建人
         |  DEPT.FNAME PURCHASEDEPT,--采购部门
         |  SALE.FNAME SALNAME,--销售员
         |  EMP.F_PAEZ_TEXT SALCOMPLANY,--销售员所属销售公司
         |  SAL.F_PAEZ_TEXT,--销售订单号
         |  SL.F_PXDF_TEXT PROJECTNO,--项目编码
         |  SL.F_PXDF_TEXT1 PROJECTNAME, --项目名称
         |  CAST(from_unixtime(unix_timestamp(sal.FCREATEDATE, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd') AS DECIMAL(18,2)) - CAST(from_unixtime(unix_timestamp(b.FAPPROVEDATE, 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd') AS DECIMAL(18,2)) AS DIFFTIME --相差天数
         |FROM
         |  salorder SAL
         |left join
         |  req b on b.fid = SAL.fsbillid
         | AND b.fentryid = SAL.fsid
         |left join ${TableName.DIM_SUPPLIER} SUP ON SAL.FSUPPLIERID=SUP.fsupplierid
         |LEFT JOIN ${TableName.DIM_SUPPLIERCONTACT} SUPC ON SAL.FPROVIDERCONTACTID = SUPC.FCONTACTID
         |LEFT JOIN ${TableName.DIM_PAEZ_ENTRY100020} ENT ON SAL.F_PAEZ_BASE=ENT.fid
         |LEFT JOIN ${TableName.DIM_UNIT} UNIT ON SAL.FUNITID=UNIT.funitid
         |LEFT JOIN ${TableName.DIM_STOCK} STOCK ON SAL.F_PAEZ_BASE2 = STOCK.fstockid
         |LEFT JOIN ${TableName.DIM_USER} USER ON SAL.FCREATORID = USER.fuserid
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} DEPT ON SAL.FPURCHASEDEPTID = DEPT.fdeptid
         |LEFT JOIN ${TableName.DIM_SALEMAN} SALE ON SAL.F_PAEZ_BASE1 = SALE.fid
         |LEFT JOIN ${TableName.DIM_EMPINFO} EMP ON SALE.FNUMBER = EMP.fnumber
         |left JOIN SALORDER_L SL ON SAL.F_PAEZ_TEXT = SL.FBILLNO AND SAL.FMATERIALID = SL.FNUMBER
         |WHERE b.FBILLNO IS NOT NULL
         |""".stripMargin)
    println("result========="+result.count())
    val table = "ads_oth_applicationtoordertime"
    MysqlConnect.overrideTable(table,result)
  }
}

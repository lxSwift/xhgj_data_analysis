package com.xhgj.bigdata.otherProject

import com.xhgj.bigdata.util.TableName
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/12/5 16:13
 * @PackageName:com.xhgj.bigdata.otherProject
 * @ClassName: ReceivableTable
 * @Description: TODO
 * @Version 1.0
 */
object ReceivableTable {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job ReceivableTable.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)

    spark.stop()
  }
  def runRES(spark: SparkSession): Unit = {

    //获取mpm表的数据
    spark.sql(
      s"""
         |select
         |  ch.c_code mcode, --物料（编码）
         |  nvl(ec.c_name,'') examinename --考核类别（名称）
         |from
         |  ODS_XHGJ.ODS_MPM_CHILD ch
         |left join ODS_XHGJ.ODS_MPM_EXAMINE_CATEGORY ec on ch.c_examine_category = ec.c_code
         |WHERE (ch.c_category_two <> 'ea8ebef464c5c8c90164ca9e2bac0015'
         |	OR ch.c_category_two IS NULL) and ch.c_code != ''
         |""".stripMargin).createOrReplaceTempView("mpmmaterial")


    spark.sql(
      s"""
         |SELECT
         |  OER.FDATE c_business_date, --业务日期
         |  OER.FBILLNO c_vatinvoice_no ,--增值税发票号
         |  DC.fname c_customer_name , --客户
         |  ORG2,fname c_sale_org , --销售组织
         |  OERE.F_PAEZ_TEXT c_sale_no,--销售单号
         |  MAT.FNUMBER c_material_code,--物料编码
         |  ENT.FNAME c_material_brand,--品牌
         |  MAT.FNAME C_MATERIAL_NAME,--物料名称
         |  MAT.FSPECIFICATION c_specification, --规格型号
         |  OERE.FPRICEQTY c_quantity,--数量
         |  DSA.fname c_salename, --销售员
         |  BUY.fname c_purchaser,--采购员
         |  ORG3.fname c_procure_department,--采购部门
         |  MPM.examinename AS c_assessment_category, --考核类别
         |  OERE.FALLAMOUNTFOR c_saletaxamount,--含税金额
         |  OERE.FNOTAXAMOUNTFOR c_saleamount --不含税金额
         |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} ORG ON OER.FPURCHASEORGID = ORG.forgid
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} ORG2 ON OER.FSALEORGID = ORG2.forgid
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON OERE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_PAEZ_ENTRY100020} ENT ON MAT.F_PAEZ_BASE=ENT.fid
         |LEFT JOIN ${TableName.DIM_SALEMAN} DSA ON OERE.F_PAEZ_BASE2 = DSA.FID
         |LEFT JOIN ${TableName.DIM_BUYER} BUY ON OERE.F_PAEZ_BASE1 = BUY.fid
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} ORG3 ON OERE.F_PAEZ_BASE = ORG3.fdeptid
         |LEFT JOIN mpmmaterial MPM ON MAT.fnumber = MPM.mcode
         |WHERE ORG.fname = '万聚国际（杭州）供应链有限公司' AND OER.FDOCUMENTSTATUS = 'C'
         |""".stripMargin)

    val table = "ads_oth_receivable"

  }
}

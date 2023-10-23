package com.xhgj.bigdata.otherProject

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/10/9 17:11
 * @PackageName:com.xhgj.bigdata.otherProject
 * @ClassName: SupplierOrderData
 * @Description: 供应商订货数据报表
 * @Version 1.0
 */
object SupplierOrderData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job SupplierOrderData.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //    salman(spark)
    //关闭SparkSession
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

    /**
     * FMANUALCLOSE 手工关闭：0否  1是
     * 采购组织：万聚国际（杭州）供应链有限公司、杭州咸亨国际应急救援装备有限公司
     * 单据状态FDOCUMENTSTATUS：已审核
     * 业务终止fmrpterminatestatus：A' then '正常' , 'B' then '业务终止'
     */
    spark.sql(
      s"""
         |SELECT
         |  OEP.FBILLNO c_billno, --单据编号
         |  '' c_source_no, --源单编号
         |  OEP.FAPPROVEDATE c_approve_date , --审核日期
         |  MAT.fnumber c_material_no , --物料编码
         |  MPM.examinename AS c_assessment_category, --考核类别
         |  OEPF.FAMOUNT_LC c_amount,--金额(本位币)
         |  DS.FNAME c_supplier_name , --供应商
         |  "采购订单" c_document_mark --单据标识
         |FROM ${TableName.ODS_ERP_POORDER} OEP
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY} OEPE ON OEP.FID = OEPE.FID
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY_F} OEPF on OEPE.fentryid = OEPF.fentryid
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON OEPE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_CUST100501} F ON MAT.f_khr = F.FID
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} ORG ON OEP.FPURCHASEORGID = ORG.forgid
         |LEFT JOIN ${TableName.DIM_SUPPLIER} DS ON OEP.FSUPPLIERID = DS.FSUPPLIERID
         |LEFT JOIN mpmmaterial MPM ON MAT.fnumber = MPM.mcode
         |where ORG.fname IN ('万聚国际（杭州）供应链有限公司','杭州咸亨国际应急救援装备有限公司') and OEP.FDOCUMENTSTATUS = 'C' and OEP.FMANUALCLOSE = '0'
         |  and OEPE.fmrpterminatestatus = 'A'
         |""".stripMargin).createOrReplaceTempView("pur_order")


    /**
     * 通过采购入库单关联采购订单
     */

    spark.sql(
      s"""
         |SELECT
         |  oei.fid,
         |  oeie.FENTRYID,
         |  DPP.fbillno
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_LK} OEIL ON OEIE.FENTRYID = OEIL.FENTRYID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oeie.FLOT = dl.FLOTID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oeie.FMATERIALID = MAT.FMATERIALID
         |JOIN ${TableName.DWD_PUR_POORDER} DPP ON  OEIL.FSBILLID = DPP.FID and OEIL.FSID = DPP.FENTRYID
         |""".stripMargin).createOrReplaceTempView("pur_instock")

    /**
     * 采购退货单相关信息
     */
    spark.sql(
      s"""
         |SELECT
         |  MRB.FBILLNO c_billno,
         |  COALESCE(PUR.fbillno,'') c_source_no,
         |  MRB.FAPPROVEDATE c_approve_date,
         |  MAT.FNUMBER c_material_no,
         |  '' c_assessment_category,
         |  MRBF.FAMOUNT_LC c_amount,
         |  '' c_supplier_name,
         |  '采购退货' c_document_mark
         |FROM
         |${TableName.ODS_ERP_MRB_DA} MRB
         |JOIN ${TableName.ODS_ERP_MRBENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.ODS_ERP_MRBENTRY_F_DA} MRBF ON MRBE.FENTRYID = MRBF.FENTRYID
         |LEFT JOIN ${TableName.ODS_ERP_MRBENTRY_LK_DA} MRBEL ON MRBE.FENTRYID = MRBEL.FENTRYID
         |LEFT JOIN pur_instock PUR ON MRBEL.FSBILLID = PUR.FID and MRBEL.FSID = PUR.FENTRYID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} ORG ON MRB.FPURCHASEORGID = ORG.forgid
         |WHERE ORG.fname IN ('万聚国际（杭州）供应链有限公司','杭州咸亨国际应急救援装备有限公司') AND MRB.FDOCUMENTSTATUS = 'C'
         |""".stripMargin).createOrReplaceTempView("mrb")

    val result = spark.sql(
      s"""
         |SELECT
         |  *
         |FROM
         |pur_order
         |UNION ALL
         |SELECT
         |  *
         |FROM
         |mrb
         |""".stripMargin)

    val tableName = "ads_pur_supplierorder"

    MysqlConnect.overrideTable(tableName,result)
  }
}

package com.xhgj.bigdata.otherProject

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/9/14 11:38
 * @PackageName:com.xhgj.bigdata.otherProject
 * @ClassName: EntryStoreToOrderTime
 * @Description: 采购订单到采购入库单的耗时情况
 * @Version 1.0
 */
object EntryStoreToOrderTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job EntryStoreToOrderTime.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession): Unit = {
    // 采购入库单数据拉取
    val result = spark.sql(
      s"""
         |SELECT
         |  INS.FBILLNO c_entrywarehous_code,--单据编号
         |  sup.fname c_supplier_name,--供应商名称
         |  C.FNUMBER c_material_code,--物料编码
         |  C.fname c_material_name,--物料名称
         |  ENT.FNAME c_material_brand,--物料品牌
         |  DEPT.FNAME c_procure_department,--采购部门
         |  H.FNAME  c_purchaser,--采购员
         |  USER.FNAME c_creater,--创建人
         |  INS.FAPPROVEDATE c_entrywarehous_audittime , --入库单审核时间
         |  INSE.FPOORDERNO c_purchase_order, --采购订单编号
         |  OEP.FAPPROVEDATE c_poorder_audittime,--采购订单审核时间
         |  (unix_timestamp(cast(INS.FAPPROVEDATE as timestamp)) - unix_timestamp(cast(OEP.FAPPROVEDATE as timestamp))) / 3600 / 24.0 AS DIFFTIME --相差天数
         |FROM
         |${TableName.ODS_ERP_INSTOCK} INS
         |JOIN ${TableName.ODS_ERP_INSTOCKENTRY} INSE on INS.fid = INSE.fid
         |left join ${TableName.DIM_ORGANIZATIONS} org on INS.FPURCHASEORGID=org.forgid
         |left join ${TableName.DIM_STOCK} sto on INSE.FSTOCKID = sto.fstockid
         |left join ${TableName.DIM_SUPPLIER} sup on INS.FSUPPLIERID = sup.fsupplierid
         |LEFT JOIN ${TableName.DIM_MATERIAL} C ON INSE.FMATERIALID = C.FMATERIALID
         |LEFT JOIN ${TableName.DIM_PAEZ_ENTRY100020} ENT ON C.F_PAEZ_BASE=ENT.fid
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} DEPT ON INS.FPURCHASEDEPTID = DEPT.fdeptid
         |LEFT JOIN ${TableName.DIM_BUYER} H ON INS.FPURCHASERID = H.FID
         |LEFT JOIN ${TableName.DIM_USER} USER ON INS.FCREATORID = USER.fuserid
         |LEFT JOIN ${TableName.ODS_ERP_POORDER} OEP ON INSE.FPOORDERNO = OEP.fbillno
         |WHERE org.fname in ('万聚国际（杭州）供应链有限公司','杭州咸亨国际应急救援装备有限公司') and sto.fname IN('直销库','应急直销库') and INS.FDOCUMENTSTATUS = 'C'
         |""".stripMargin)

    println("num============"+result.count())

    val table = "ads_oth_entrystoretoordertime"
    MysqlConnect.overrideTable(table, result)

  }

}

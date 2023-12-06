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
     * 获取手工的2022-12-31日期以前的已开票应收款余额 FLOT
     */
    val sc = spark.sparkContext
    PathUtil.deleteExistedPath(sc,"/data/file/test")

    spark.sql(
      s"""
         |select
         |  INSE.F_PAEZ_TEXT,
         |  MAT.fnumber c_material_no, --物料编码
         |  if(COALESCE(INSE.FREALQTY,0) != '0' or trim(INSE.FREALQTY) != '',COALESCE(oeif.FALLAMOUNT_LC,0) / INSE.FREALQTY,0) FALLAMOUNT_LC, --含税单价
         |  if(COALESCE(INSE.FREALQTY,0) != '0' or trim(INSE.FREALQTY) != '',COALESCE(oeif.FAMOUNT_LC,0) / INSE.FREALQTY,0) FAMOUNT_LC, --除税单价
         |  sup.fname c_supplier_name, --供应商名称
         |  dl.fnumber FLOT --批号
         |from
         |${TableName.ODS_ERP_INSTOCK} INS
         |JOIN ${TableName.ODS_ERP_INSTOCKENTRY} INSE on INS.fid = INSE.fid
         |left join ${TableName.ODS_ERP_INSTOCKENTRY_F} oeif ON INSE.FENTRYID = oeif.FENTRYID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON INSE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON INSE.FLOT = dl.FLOTID
         |left join ${TableName.DIM_ORGANIZATIONS} org on INS.FSTOCKORGID=org.forgid
         |left join ${TableName.DIM_SUPPLIER} sup on INS.FSUPPLIERID = sup.fsupplierid
         |WHERE org.fname = '万聚国际（杭州）供应链有限公司' and INS.FDOCUMENTSTATUS = 'C'
         |and INS.FAPPROVEDATE >= '2022-01-01'
         |""".stripMargin).coalesce(1).write.csv("/data/file/test")



//      .coalesce(1).write.csv("/data/file/instock")


  }






}

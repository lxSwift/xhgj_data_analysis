package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/8/23 10:05
 * @PackageName:com.xhgj.bigdata.textProject
 * @ClassName: ceshi2
 * @Description: TODO
 * @Version 1.0
 */
object ceshi2 {
  val takeRow = 20

  case class Params(inputDay: String = null)

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
  def runRES(spark: SparkSession)={

    //FISOVERLEGALORG组织间结算跨法人标识 为1 代表是   0代表否
    spark.sql(
      s"""
         |SELECT
         |  MRB.FBILLNO c_billno,
         |  MAT.FNUMBER c_material_no,
         |  MRBE.FENTRYID
         |FROM
         |${TableName.ODS_ERP_MRB_DA} MRB
         |JOIN ${TableName.ODS_ERP_MRBENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.ODS_ERP_MRBFIN_DA} MRBF ON MRB.FID = MRBF.FID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} ORG ON MRB.FPURCHASEORGID = ORG.forgid
         |WHERE ORG.fname IN ('万聚国际（杭州）供应链有限公司','杭州咸亨国际应急救援装备有限公司') AND MRB.FDOCUMENTSTATUS = 'C' and MRB.FBILLNO = 'CGTL005047'
         |""".stripMargin).show(100,false)






  }

}

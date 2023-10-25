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
         |  MAT.fnumber, --物料编码
         |  dl.fnumber FLOT, --批号
         |  oei.fapprovedate,
         |  oei.FDOCUMENTSTATUS,
         |  oeie.FSTOCKFLAG
         |FROM ods_xhgj.ODS_ERP_INSTOCK oei
         |LEFT JOIN ods_xhgj.ODS_ERP_INSTOCKENTRY oeie ON oei.FID = oeie.FID
         |LEFT JOIN dw_xhgj.DIM_MATERIAL MAT ON oeie.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN dw_xhgj.DIM_LOTMASTER dl ON oeie.FLOT = dl.FLOTID
         |LEFT JOIN dw_xhgj.DIM_STOCK DS ON oeie.FSTOCKID = DS.FSTOCKID
         |where dl.fnumber = '2023091403441'
         |""".stripMargin).show(100,false)



  }

}

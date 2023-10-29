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
         |  MAT.FNUMBER c_material_code,--物料编码
         |  dl.fnumber c_flot_no, --批号
         |  MRB.*
         |FROM
         |${TableName.ODS_ERP_INVENTORY_DA} MRB
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRB.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRB.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRB.FLOT = dl.FLOTID
         |WHERE DS.fname = '海宁1号库' AND MAT.FNUMBER = 'BE030129' AND dl.fnumber = '202104060289'
         |""".stripMargin).show(10,false)




  }

}

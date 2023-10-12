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
         |  MRBE.FREALQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot,
         |  MRB.FAPPROVEDATE
         |FROM
         |${TableName.ODS_ERP_OUTSTOCK} MRB
         |JOIN ${TableName.ODS_ERP_OUTSTOCKENTRY} MRBE ON MRB.FID = MRBE.FID
         |JOIN ${TableName.ODS_ERP_OUTSTOCKFIN} MRBF ON MRB.FID = MRBF.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE MRB.FSTOCKORGID IN ('1','481351') AND MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','应急海宁1号库','应急海宁2号库','嘉峪关分仓','惠州分仓') AND MRBF.FISGENFORIOS = '0' and  dl.fnumber ='202012170595'
         |""".stripMargin).show(10,false)





  }

}

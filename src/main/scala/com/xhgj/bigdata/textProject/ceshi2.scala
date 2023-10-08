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

    spark.sql(
      s"""
         |SELECT
         |  MRBE.FRMREALQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot
         |FROM
         |${TableName.ODS_ERP_MRB_DA} MRB
         |JOIN ${TableName.ODS_ERP_MRBENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE MRB.FSTOCKORGID IN ('1','481351') AND MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','应急海宁1号库','应急海宁2号库','嘉峪关分仓','惠州分仓') and MAT.FNUMBER = 'AB060030' and dl.fnumber = '202004260071'
         |""".stripMargin).show(10,false)

    spark.sql(
      s"""
         |SELECT
         |  MRBE.FREALQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot
         |FROM
         |${TableName.ODS_ERP_OUTSTOCK} MRB
         |JOIN ${TableName.ODS_ERP_OUTSTOCKENTRY} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE MRB.FSTOCKORGID IN ('1','481351') AND MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','应急海宁1号库','应急海宁2号库','嘉峪关分仓','惠州分仓') and MAT.FNUMBER = 'AB060030' and dl.fnumber = '202004260071'
         |""".stripMargin).show(10,false)

    spark.sql(
      s"""
         |SELECT
         |  MRB.FBASEAVBQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot
         |FROM
         |${TableName.ODS_ERP_INVENTORY_DA} MRB
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRB.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRB.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRB.FLOT = dl.FLOTID
         |WHERE MRB.FSTOCKORGID IN ('1','481351')
         |AND DS.fname IN ('海宁1号库','应急海宁1号库','应急海宁2号库','嘉峪关分仓','惠州分仓') and MAT.FNUMBER = 'AB060030' and dl.fnumber = '202004260071'
         |""".stripMargin).show(10,false)

  }

}

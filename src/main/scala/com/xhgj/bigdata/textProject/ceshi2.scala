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
    val result = spark.sql(
      s"""
         |select
         |	DP.fnumber PRONO,
         | CAST(SUM(OERE.FPRICEQTY * OERE.FTAXPRICE) AS INT) AS SALETAXAMOUNT --含税金额
         |from
         |	${TableName.ODS_ERP_RECEIVABLE} a
         |join ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE on a.fid=OERE.fid
         |left join ${TableName.DIM_PROJECTBASIC} DP on OERE.FPROJECTNO=DP.fid
         |left join ${TableName.DIM_ORGANIZATIONS} org on org.forgid=a.FSETTLEORGID
         |where a.FDOCUMENTSTATUS='C' AND org.fname ='DP咸亨国际科技股份有限公司'
         |group by DP.fnumber
         |having SALETAXAMOUNT = 0
         |""".stripMargin)
    println("res========="+result.count())

    result.show(100,false)


  }

}

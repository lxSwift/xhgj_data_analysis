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
         |  c_order_time,
         |  c_order_id,
         |  c_platform_price,
         |  c_title
         |FROM
         |  ${TableName.DWD_WRITE_PFORDER}
         |""".stripMargin).createOrReplaceTempView("write_pforder")

    //将履约平台的2023之后的数据取出来, 同一个订单类型里面的客户订单号是唯一的,c_state代表的是正常使用的数据, 46代表是管网的数据
    //订单状态待取消'3',取消'4',预占'6',全部订单'0'
    spark.sql(
      s"""
         |SELECT
         |  A.c_state,
         |  A.c_docking_order_no c_order_id, --客户订单号
         |  A.c_state
         |  ,A.c_docking_type
         |  ,A.c_order_time
         |  ,A.c_order_state
         |FROM
         |  ${TableName.ODS_OMS_PFORDER} A
         |LEFT JOIN ${TableName.ODS_OMS_PFRETURNORDER} B ON A.id = B.order_id and B.c_state = '1'
         |LEFT JOIN ${TableName.ODS_OMS_PFRETURNORDERDETAIL} C ON B.id = C.return_order_id and C.c_state = '1'
         |where  A.c_docking_order_no = '202309271422165430'
         |""".stripMargin).show(10,false)





  }

}

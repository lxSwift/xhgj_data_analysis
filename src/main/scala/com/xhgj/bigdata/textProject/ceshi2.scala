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

    val mysqltable = MysqlConnect.getMysqlData("ads_status_payamount",spark)
    mysqltable.show(10,false)
    val num: Long = mysqltable.count()
    print("num============="+num)

  }

}

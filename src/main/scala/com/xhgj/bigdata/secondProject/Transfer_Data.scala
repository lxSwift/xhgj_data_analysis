package com.xhgj.bigdata.secondProject
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/6/30 11:37
 * @PackageName:com.xhgj.bigdata.secondProject
 * @ClassName: Transfer_Data
 * @Description: 调拨价数据分析
 * @Version 1.0
 */
object Transfer_Data {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job PayAmount_Ready.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession): Unit = {

    //结算组织=万聚国际（杭州）供应链有限公司或=杭州咸亨国际应急救援装备有限公司，且单据状态等于已审核未作废的应收单


  }

}

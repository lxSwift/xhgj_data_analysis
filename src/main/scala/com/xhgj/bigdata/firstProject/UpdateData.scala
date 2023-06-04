package com.xhgj.bigdata.firstProject

import org.apache.derby.impl.sql.compile.TableName
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author luoxin
 * @Date 2023/5/31 8:39
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: UpdateData
 * @Description: 完成hive表的增量更新要求
 * @Version 1.0
 */
object UpdateData {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark task job UpdateData.scala")
      .enableHiveSupport()
      .getOrCreate()
    //tablename的格式是  库名.表名
    val tablename = args(0)
    println(s"The following hive table needs to be updated : ${tablename}")

    runRES(spark,tablename)
    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession,tbName:String): Unit = {
    //读取Hive表中的数据
    spark.sql(
      s"""
        |SELECT
        | *,
        | row_number() over(partition by id order by age desc) rank
        |FROM
        |${tbName}
        |""".stripMargin).createOrReplaceTempView("tt")

    spark.sql(
      s"""
        |insert overwrite table ${tbName}
        |select
        | id,
        | name,
        | age
        |from tt
        |where
        |rank = 1
        |""".stripMargin)
  }
}

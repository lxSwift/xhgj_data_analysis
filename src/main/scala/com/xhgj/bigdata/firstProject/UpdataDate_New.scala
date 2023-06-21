package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.Config
import org.apache.spark.sql.SparkSession
import com.xhgj.bigdata.util.TableName

/**
 * @Author luoxin
 * @Date 2023/6/16 11:32
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: UpdataDate_New
 * @Description: 增量更新统一表
 * @Version 1.0
 */
object UpdataDate_New {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark task job UpdateData_New.scala")
      .enableHiveSupport()
      .getOrCreate()

    //获取配置文件里需要增量的表名以及字段
    val prop =Config.load("config.properties")
    val allTableName = prop.getProperty("table_incr")
    val tbList = allTableName.split(",")
    for (i <- tbList) {
      val tbList2 = i.split("--")
      val tableName = tbList2(0)
      val key_fields = tbList2(1)
      val order_field = tbList2(2)
      println("updataTable JOB START_RUNNING:  "+tableName+"and"+key_fields+"and"+order_field)
      runRES(spark,tableName,key_fields,order_field)
    }
    //关闭SparkSession
    spark.stop()
  }

  def  runRES(spark: SparkSession,tableName:String,key_fields:String,order_field:String): Unit = {
    val tableName_tmp= tableName+"_tmp"
    //关联的上的就直接取临时表字段, 关联不上的就去正式表
    //每天只需要更新临时表即可
    spark.sql(
      s"""
         |SELECT
         |  T2.*
         |FROM
         |  ${tableName} T1
         |JOIN ${tableName_tmp} T2 ON T1.${key_fields} = T2.${key_fields}
         |UNION ALL
         |SELECT
         |  T1.*
         |FROM
         |  ${tableName} T1
         |LEFT JOIN ${tableName_tmp} T2 ON T1.${key_fields} = T2.${key_fields}
         |WHERE T2.${key_fields} IS NULL
         |""".stripMargin)

  }

}

package com.xhgj.bigdata.textProject
import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

// SQL Server JDBC依赖项
import java.sql.DriverManager
import java.sql.Connection

/**
 * @Author luoxin
 * @Date 2023/11/24 11:39
 * @PackageName:com.xhgj.bigdata.textProject
 * @ClassName: pro_ceshi
 * @Description: TODO
 * @Version 1.0
 */
object pro_ceshi {
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

//    // 连接到SQL Server数据库
//    val jdbcUrl = "jdbc:sqlserver://172.16.104.158:1433;databaseName=ekp"
//    val connectionProperties = new java.util.Properties()
//    connectionProperties.setProperty("user", "bi")
//    connectionProperties.setProperty("password", "Xhgj_OA@9000")
//    connectionProperties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
//
//    val jdbcDF = spark.read.jdbc(jdbcUrl, "sys_org_element", connectionProperties)

    val jdbcDF = spark.read.format("jdbc").option("url","jdbc:sqlserver://172.16.104.158:1433;databaseName=ekp;encrypt=false;trustServerCertificate=true").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable","sys_org_element").option("user", "bi").option("password", "Xhgj_OA@9000").load()
    println("res=========" + jdbcDF.count())

    jdbcDF.show(100, false)


  }
}

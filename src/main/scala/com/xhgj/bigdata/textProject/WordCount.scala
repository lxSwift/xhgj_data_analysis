package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.sql.Types.{VARCHAR,INTEGER,DATE}
import java.util.Properties
object WordCount {
  def main(args: Array[String]): Unit = {
    //配置不要把mysql字段注释清空
    val spark = SparkSession
      .builder()
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.sources.jdbc.connectionProperties", "rewriteBatchedStatements=false")
      .appName("Spark task job Test.scala")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = Seq(
      ("Tom", "15", "2023-06-12"),
      ("Jerry", "16", "2023-06-12")
    ).toDF("projectname", "dept", "createtime")

    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = "jdbc:mysql://172.16.104.238:3306/kettle_test?useInformationSchema=true"
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "tt4"

    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    // 写入 MySQL 数据库DATETIME
    df.write
      .mode("overwrite")
      .option("createTableColumnTypes", "projectname varchar(255) NOT NULL COMMENT '项目名称',dept varchar(128) NOT NULL COMMENT '部门',createtime timestamp NOT NULL COMMENT '创建时间'")  // 明确指定 MySQL 数据库中字段的数据类型
      .option("batchsize", "10000")
      .option("truncate", "false")
      .option("jdbcType", s"projectname=${VARCHAR}, dept =${VARCHAR},createtime=${DATE}")   // 显式指定 SparkSQL 中的数据类型和 MySQL 中的映射关系
      .jdbc(url, table, props)

    //关闭SparkSession
    spark.stop()
  }

}

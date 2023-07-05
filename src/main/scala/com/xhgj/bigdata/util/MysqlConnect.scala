package com.xhgj.bigdata.util

import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Types.{VARCHAR,DECIMAL,INTEGER}
import java.sql.{DriverManager, Statement}
import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/12 9:40
 * @PackageName:com.xhgj.bigdata.util
 * @ClassName: MysqlConnect
 * @Description: mysql链接示例
 * @Version 1.0
 */
object MysqlConnect {
  // 定义 MySQL 的连接信息
  val conf = Config.load("config.properties")
  val url = conf.getProperty("database.url")
  val user = conf.getProperty("database.user")
  val password = conf.getProperty("database.password")
  val driver = "com.mysql.jdbc.Driver"
  // 定义 JDBC 的相关配置信息
  val props = new Properties()
  props.setProperty("user", user)
  props.setProperty("password", password)
  props.setProperty("driver", driver)
  Class.forName(driver)
  /**
   * 覆盖导出至mysql表中
   * @param tableName
   * @param result
   */
  def overrideTable(tableName: String,result: DataFrame): Unit = {
    result.write.mode(SaveMode.Overwrite).jdbc(url, tableName, props)
  }

  def overrideTableDateType(tableName: String,result: DataFrame,column: String,typestr:String)={
    result.write.mode("overwrite")
      .option("createTableColumnTypes", column) // 明确指定 MySQL 数据库中字段的数据类型
      .option("batchsize", "10000")
      .option("truncate", "false")
      .option("jdbcType", typestr) // 显式指定 SparkSQL 中的数据类型和 MySQL 中的映射关系
      .jdbc(url, tableName, props)
  }

  /**
   * 追加写入mysql表中
   * @param tableName
   * @param result
   */
  def appendTable(tableName: String,result: DataFrame): Unit = {
    result.write.mode(SaveMode.Append).jdbc(url, tableName, props)
  }

  /**
   * 对mysql的表执行sql语句
   * @param sql
   */
  def executeUpdateTable(sql:String): Unit = {
    ////获取与mysql的连接, mysql的jdbc包要先准备好
    val conn = DriverManager.getConnection(url, user, password)
    val stmt: Statement = conn.createStatement()

    stmt.executeUpdate(sql)
    stmt.close()
    conn.close()
  }



}

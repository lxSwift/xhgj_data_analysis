package com.xhgj.bigdata.util

/**
 * @Author luoxin
 * @Date 2023/6/12 9:40
 * @PackageName:com.xhgj.bigdata.util
 * @ClassName: MysqlConnect
 * @Description: mysql链接示例
 * @Version 1.0
 */
object MysqlConnect {
  import org.apache.spark.sql.{SaveMode, SparkSession}
  import java.sql.{Connection,DriverManager,Statement}

  val url = "jdbc:mysql://localhost:3306/testdb"
  val table = "person"
  val user = "root"
  val password = "password"

  val spark = SparkSession.builder
    .appName("MySQL Example")
    .getOrCreate

  val data = Seq(
    ("Alice", 25, "female"),
    ("Bob", 30, "male"),
    ("Charlie", 35, "male"),
    ("David", 40, "male"),
    ("Eva", 45, "female")
  )

  val df = spark.createDataFrame(data).toDF("name", "age", "gender")

  val jdbcTypes = Map(
    "name" -> "VARCHAR(255)",
    "age" -> "INTEGER",
    "gender" -> "ENUM('male', 'female')"
  )
  //获取与mysql的连接, mysql的jdbc包要先准备好
  val conn = DriverManager.getConnection(url, user, password)
  val stmt: Statement = conn.createStatement()

  // 使用 ALTER TABLE 语句修改 person 表的列数据类型
  jdbcTypes.foreach(column => {
    stmt.execute(s"ALTER TABLE $table MODIFY COLUMN ${column._1} ${column._2}")
  })

  // 将 DataFrame 写入 MySQL 表中
  df.write
    .mode(SaveMode.Overwrite)
    .option("driver", "com.mysql.jdbc.Driver")
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("rewriteBatchedStatements", true)
    .option("batchsize", 1000)
    .option("truncate", true)
    .jdbc(url, table, new java.util.Properties())

  stmt.close()
  conn.close()
}

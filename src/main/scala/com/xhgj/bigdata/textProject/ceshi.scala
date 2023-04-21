package scala.com.xhgj.bigdata.textProject

import org.apache.spark.sql.SparkSession

object ceshi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Hive Demo").enableHiveSupport().getOrCreate()
    spark.sql(
      """
        |SELECT
        | *
        |FROM
        |keetle_test.tt1
        |LIMIT 10
        |""".stripMargin).write.csv("hdfs://dw1:9000/spark/spark2hdfs")

    spark.stop()
  }

}

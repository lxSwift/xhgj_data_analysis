package scala.com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Hive Demo").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    //测试从配置文件读取值
    val conf = Config.load("config.properties")
    val outputPath: String = conf.getProperty("database.url")
    val use = conf.getProperty("database.user")
    val pass = conf.getProperty("database.password")
  }
}

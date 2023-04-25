package scala.com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.{Config, PathUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ceshi {
  val takeRow = 20

  case class Params(inputDay: String =null)
  def main(args: Array[String]): Unit = {
    val inputDay = args(0)  //接受输入的日期参数
    val params = Params(inputDay)
    run(params)
  }

  def run(params:Params): Unit = {
    val spark = SparkSession.builder().appName("Spark Hive Demo").enableHiveSupport().getOrCreate()
    val sc=spark.sparkContext
    //测试从配置文件读取值
    val conf = Config.load("my.properties")
    val outputPath: String = conf.getProperty("outputPath")
    //判断路径是否存在,存在即删除
    if (PathUtil.isExisted(sc,outputPath)){
      PathUtil.deleteExistedPath(sc,outputPath)
    }else{
      println("outputPath is not exist")
    }

    val result=runRES(spark,params)
    result.show(takeRow)
    result.write.csv(outputPath)
    sc.stop()
    spark.stop()

  }

  def runRES(spark: SparkSession, params: Params):DataFrame = {

    import spark.implicits._

    val result: DataFrame = spark.sql(
      s"""
         |SELECT
         | ${params.inputDay},
         | *
         |FROM
         |keetle_test.tt1
         |LIMIT 10
         |""".stripMargin)
    result
  }
  def showDf(result:DataFrame,num:Int): Unit = {
    result.show(num)
  }
}

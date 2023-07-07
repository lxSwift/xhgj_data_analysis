package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{Config, TableName}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Types.{DECIMAL, VARCHAR}
import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/15 10:05
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: EasMon
 * @Description: eas中的其他应付款\应付款\其他应收款的期末余额以及年初余额获取
 * @Version 1.0
 */
object EasMon {

  case class Params(inputMonth: String =null)
  def main(args: Array[String]): Unit = {
    val inputMonth = args(0) //接受输入的日期参数
    val params = Params(inputMonth)
    run(params)
  }
  def run(params:Params): Unit ={
    val spark = SparkSession.builder().appName("Spark EasMon Demo").enableHiveSupport().getOrCreate()
    runRES(spark,params)
    spark.stop()
  }
  def runRES(spark: SparkSession, params: Params): Unit = {
    val inputMonth = params.inputMonth
    val yearbegin = inputMonth.substring(0,4)+"01"
    val conf = Config.load("config.properties")
    val kmnumber = conf.getProperty("km.FNUMBER")
    val bbfnumber = conf.getProperty("bb.FNUMBER")
    val yefbaltype = conf.getProperty("ye.FBALTYPE")
    val gsfnumber = conf.getProperty("gs.FNUMBER")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val tablename = "ads_fin_accountbalance"
    /**
     * 参数说明
     * --ye.FPERIOD为期间, 表示的是其他应收款、应付款和其他应付款的累计时间节点
     * --km.FNUMBER为科目编码， 1221 2202 2241分别表示其他应收款、应付款、其他应付款
     * --bb.FNUMBER为币种的编码  GLC代表综合本位币
     * --ye.FBALTYPE余额类型, 1－保存后余额，5－过账后余额
     * --gs.FNUMBER公司编码  10503代表的是电商公司
     */
    //进行列裁剪,先过滤出期间为上个月以及年初的数据
    spark.sql(
      s"""
         |select
         |  FPERIOD,
         |  FORGUNITID,
         |  FACCOUNTID,
         |  FCURRENCYID,
         |  FEndBalanceLocal,
         |  FDEBITLOCAL,
         |  FCREDITLOCAL,
         |  FBeginBalanceRpt,
         |  FBALTYPE
         |from ${TableName.ODS_EAS_ACCOUNTBALANCE}
         |where FPERIOD=${inputMonth} or FPERIOD=${yearbegin}
         |""".stripMargin).createOrReplaceTempView("ACCOUNTBALANCE")
    //获取年初余额值
    spark.sql(
      s"""
         |SELECT km.FNUMBER FNUMBER,ye.FBeginBalanceRpt yearBeginBalance
         |FROM ACCOUNTBALANCE  ye
         |left join ${TableName.DIM_COMPANY} gs on ye.FORGUNITID = gs.fid
         |left join ${TableName.DIM_ACCOUNTVIEW} km on ye.FACCOUNTID = km.fid
         |left join ${TableName.DIM_CURRENCY}  bb on ye.FCURRENCYID = bb.fid
         |where ye.FPERIOD=${yearbegin} and km.FNUMBER in ${kmnumber} and bb.FNUMBER=${bbfnumber} and ye.FBALTYPE=${yefbaltype} and gs.FNUMBER=${gsfnumber}
         |""".stripMargin).createOrReplaceTempView("year_b")

    val res = spark.sql(
      s"""
         |SELECT
         |  gs.fname_l2 COMPANYNAME,  --公司名称
         |  ye.FPERIOD FPERIOD,   --期间
         |  km.fname_l2 KMNAME,   --科目名称
         |  km.FNUMBER KMID,  --科目编码
         |  ye.FDEBITLOCAL FDEBITLOCAL,     --本期借方本位币
         |  ye.FCREDITLOCAL FCREDITLOCAL,   --本期贷方本位币
         |  abs(ye.FEndBalanceLocal) FENDBALANCELOCAL, --当前期末月余额
         |  abs(ye.FBeginBalanceRpt) FBEGINBALANCERPT, --当前期初月余额
         |  abs(yb.yearBeginBalance) YEARBEGINBALANCE --年初余额
         |FROM ACCOUNTBALANCE  ye
         |left join ${TableName.DIM_COMPANY}  gs on ye.FORGUNITID =gs.fid
         |left join ${TableName.DIM_ACCOUNTVIEW} km on ye.FACCOUNTID =km.fid
         |left join ${TableName.DIM_CURRENCY}  bb on ye.FCURRENCYID=bb.fid
         |left join year_b yb on yb.FNUMBER =km.FNUMBER
         |where ye.FPERIOD=${inputMonth} and km.FNUMBER in ${kmnumber} and bb.FNUMBER=${bbfnumber} and ye.FBALTYPE=${yefbaltype} and gs.FNUMBER=${gsfnumber}
         |""".stripMargin)
    res.show(10)

    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res.write.mode("overwrite")
      .option("createTableColumnTypes", "COMPANYNAME varchar(255),FPERIOD varchar(10),KMNAME varchar(255),KMID varchar(25),FDEBITLOCAL decimal(19,4),FCREDITLOCAL decimal(19,4),FENDBALANCELOCAL decimal(19,4),FBEGINBALANCERPT decimal(19,4),YEARBEGINBALANCE decimal(19,4)") // 明确指定 MySQL 数据库中字段的数据类型
      .option("batchsize", "10000")
      .option("truncate", "false")
      .option("jdbcType", s"COMPANYNAME=${VARCHAR},FPERIOD=${VARCHAR},KMNAME=${VARCHAR},KMID=${VARCHAR},FDEBITLOCAL=${DECIMAL},FCREDITLOCAL=${DECIMAL},FENDBALANCELOCAL=${DECIMAL},FBEGINBALANCERPT=${DECIMAL},YEARBEGINBALANCE=${DECIMAL}") // 显式指定 SparkSQL 中的数据类型和 MySQL 中的映射关系
      .jdbc(url, tablename, props)
  }

}

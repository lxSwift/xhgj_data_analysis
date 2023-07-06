package com.xhgj.bigdata.micro

import com.xhgj.bigdata.util.{Config, TableName}
import org.apache.spark.sql.SparkSession

import java.sql.Types.VARCHAR
import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/12 17:27
 * @PackageName:com.xhgj.bigdata.micro
 * @ClassName: PayAmount_Status
 * @Description: 回款状态
 * @Version 1.0
 */
object PayAmount_Status {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job PayAmount_Status.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession)={
    //应收单相关信息
    /**
     * 回款状态不包括内部客户
     */
    spark.sql(
      s"""
         |select
         |  prono,
         |  proname,
         |  salename,
         |  fnumber_sal,
         |  sum(cast(allamountfor as decimal(19,4))) ys_amount
         |from
         |  ${TableName.DWS_RECE_PAYAMOUNT}
         |where
         |fnumber_cus NOT IN('XH00001','XH00002','XH00003','XH00004','XH00005','XH00006','XH00007','XH00008','XH00009','XH00010','XH00011','XH00012','XH00013','XH00014','XH00015','XH00016','XH00017','XH00018','XH00019','XH00020','XH00021','XH00022','XH00023','XH00024','XH00025','XH00026','XH00027','XH00028','XH00029','XH00030','XH00031','XH00032','XH00033','XH00034','XH00035','XH00036','XH00037','XH00038','XH00039','XH00040','XH00041','XH00042','XH00043','XH00044','XH00045','XH00046','XH00047','XH00048','XH00049','XH00050','XH00051','XH00052','XH00053','XH00054','XH00055','XH00056','XH00057','XH00058','XH00059','XH00060','XH00061','XH00062','XH00063','XH00064','XH00065','XH00180','XH00192','XH00196','XH00197','XH00198','XH00199','XH00200','XH00201','XH00202','XH00203','XH00204','XH00205','XH00206','XH00207','XH00208','XH00209','XH00210','XH00211','XH00212','XH00213','XH00214','XH00215','DPXHNB00001','DPXHNB00002')
         |group by prono,proname,salename,fnumber_sal
         |""".stripMargin).createOrReplaceTempView("receivable")

    //收款单相关信息
    spark.sql(
      s"""
         |select
         |  FPROJECTNO,
         |  PRONAME,
         |  sum(cast(FREALRECAMOUNTFOR as decimal(19,4))) sk_amount
         |from
         |  ${TableName.DWS_RECE_PAYAMOUNTGET}
         |where FPROJECTNO is not null and
         | fnumber_cus NOT IN('XH00001','XH00002','XH00003','XH00004','XH00005','XH00006','XH00007','XH00008','XH00009','XH00010','XH00011','XH00012','XH00013','XH00014','XH00015','XH00016','XH00017','XH00018','XH00019','XH00020','XH00021','XH00022','XH00023','XH00024','XH00025','XH00026','XH00027','XH00028','XH00029','XH00030','XH00031','XH00032','XH00033','XH00034','XH00035','XH00036','XH00037','XH00038','XH00039','XH00040','XH00041','XH00042','XH00043','XH00044','XH00045','XH00046','XH00047','XH00048','XH00049','XH00050','XH00051','XH00052','XH00053','XH00054','XH00055','XH00056','XH00057','XH00058','XH00059','XH00060','XH00061','XH00062','XH00063','XH00064','XH00065','XH00180','XH00192','XH00196','XH00197','XH00198','XH00199','XH00200','XH00201','XH00202','XH00203','XH00204','XH00205','XH00206','XH00207','XH00208','XH00209','XH00210','XH00211','XH00212','XH00213','XH00214','XH00215','DPXHNB00001','DPXHNB00002')
         |group by FPROJECTNO,PRONAME
         |""".stripMargin).createOrReplaceTempView("sktable")

    //找到项目收款最新的业务日期
    spark.sql(
      s"""
         |select
         |  FPROJECTNO,
         |  fdate
         |from (
         |SELECT
         |  FPROJECTNO,
         |  fdate,
         |  ROW_NUMBER() OVER(PARTITION BY FPROJECTNO ORDER BY FDATE DESC) AS RANKNUM
         |FROM
         |  ${TableName.DWS_RECE_PAYAMOUNTGET}
         |where FPROJECTNO is not null and
         | fnumber_cus NOT IN('XH00001','XH00002','XH00003','XH00004','XH00005','XH00006','XH00007','XH00008','XH00009','XH00010','XH00011','XH00012','XH00013','XH00014','XH00015','XH00016','XH00017','XH00018','XH00019','XH00020','XH00021','XH00022','XH00023','XH00024','XH00025','XH00026','XH00027','XH00028','XH00029','XH00030','XH00031','XH00032','XH00033','XH00034','XH00035','XH00036','XH00037','XH00038','XH00039','XH00040','XH00041','XH00042','XH00043','XH00044','XH00045','XH00046','XH00047','XH00048','XH00049','XH00050','XH00051','XH00052','XH00053','XH00054','XH00055','XH00056','XH00057','XH00058','XH00059','XH00060','XH00061','XH00062','XH00063','XH00064','XH00065','XH00180','XH00192','XH00196','XH00197','XH00198','XH00199','XH00200','XH00201','XH00202','XH00203','XH00204','XH00205','XH00206','XH00207','XH00208','XH00209','XH00210','XH00211','XH00212','XH00213','XH00214','XH00215','DPXHNB00001','DPXHNB00002')
         | ) a where a.RANKNUM =1
         |""".stripMargin).createOrReplaceTempView("datenew")

    //获取最终状态并保存至mysql
    val res = spark.sql(
      s"""
         |select
         |  a.prono,
         |  a.proname,
         |  case
         |    when ys_amount-nvl(sk_amount,0) <= 0 and ys_amount != 0 and nvl(sk_amount,0) != 0 then "全部回款"
         |    when ys_amount-nvl(sk_amount,0) > 0 and (ys_amount-nvl(sk_amount,0)) < ys_amount then "部分回款"
         |    when nvl(sk_amount,0)=0 then "未回款"
         |    else "其他状态" end as paystatus,
         |   salename,
         |   fnumber_sal,
         |   ys_amount,
         |   nvl(sk_amount,0) sk_amount,
         |   ys_amount-nvl(sk_amount,0) diff_amount,
         |   nvl(c.fdate,'') fdate
         |from
         |  receivable a
         |left join sktable b on a.prono=b.FPROJECTNO
         |left join datenew c on a.prono=c.FPROJECTNO
         |""".stripMargin)

    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_status_payamount"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中
    res.write.mode("overwrite")
      .option("createTableColumnTypes", "prono varchar(255),proname varchar(2000),paystatus varchar(25)") // 明确指定 MySQL 数据库中字段的数据类型
      .option("batchsize", "10000")
      .option("truncate", "false")
      .option("jdbcType", s"prono=${VARCHAR},proname=${VARCHAR},paystatus=${VARCHAR}") // 显式指定 SparkSQL 中的数据类型和 MySQL 中的映射关系
      .jdbc(url, table, props)
  }
}

package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.TableName
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/8 9:00
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: Test
 * @Description: TODO
 * @Version 1.0
 */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job Test.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession): Unit = {
    val res = spark.sql(
      s"""
         |select oes.fcreatedate as createdate,
         | dp.fname as projectname,
         | dp.fnumber as projectno ,
         | dp.fcustomerorderid as customerorderid,
         | oes.f_paez_text13 as salescompany ,
         | oes.f_paez_text14 as salesdept,
         | du.fname as createuser,
         | ds.fname as salename,
         | de.fname as operatorname,
         | dm.fnumber as materialno,
         | dm.f_paez_text as brand,
         | dm.fname as materialname,
         | dm.fspecification as specification,
         | oese.fqty as qty,
         | oesf.ftaxprice as taxprice,
         | oesf.fallamount as allamount,
         | oes.f_paez_text as consignee,
         | oes.f_paez_text1 as contactphone,
         | oes.f_paez_text2 as shppingaddress,
         | dc.f_paez_checkbox as isinternalcustomer,
         | oes.fpurtype as purtype,
         | oese.fmrpterminatestatus as mrpterminatestatus,
         | oes.f_paez_checkbox as isdx,
         | case when (oes.f_paez_checkbox = 1 OR oes.fbillno like 'HZXM%') then "非自营"
         | else "自营" end AS performanceform
         |from ${TableName.ODS_ERP_SALORDER} oes
         |left join ${TableName.ODS_ERP_SALORDERENTRY} oese on oes.fid = oese.fid
         |left join ${TableName.DIM_PROJECTBASIC} dp on oes.fprojectbasic  = dp.fid
         |left join ${TableName.DIM_USER} du on oes.fcreator = du.fuserid
         |left join ${TableName.DIM_SALEMAN} ds on oes.fsalerid = ds.fid
         |left join ${TableName.DIM_EMPINFO} de on oes.f_paez_base3 = de.fid
         |left join ${TableName.DIM_MATERIAL} dm on oese.fmaterialid = dm.fmaterialid
         |left join ${TableName.ODS_ERP_SALORDERENTRY_F} oesf on oesf.fentryid = oese.fentryid
         |left join ${TableName.DIM_CUSTOMER} dc on oes.fcustid = dc.fcustid
         |""".stripMargin)
    println(res.count())
    // 定义 MySQL 的连接信息
    val url = "jdbc:mysql://172.16.104.238:3306/ads_xhgj"
    val table = "ads_sale_performance"
    val username = "root"
    val password = "1qaz@WSX"

    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", username)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中
    res.write.mode("overwrite").jdbc(url, table, props)
  }

}



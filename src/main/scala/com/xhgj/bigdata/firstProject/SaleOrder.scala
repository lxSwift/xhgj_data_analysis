package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{Config, TableName}

import org.apache.spark.sql.SparkSession
import java.sql.Types.{VARCHAR,DECIMAL,INTEGER}
import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/8 9:00
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: Test
 * @Description: 销售订单报表编写
 * @Version 1.0
 */
object SaleOrder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job SaleOrder.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession): Unit = {
    /**
     * 销售履约订单报表编写;
     * 订单类型取不为1, 取不是大票的 2即小票 3即供应链订单
     */
    val res = spark.sql(
      s"""
         |select cast (oes.fcreatedate as varchar(32)) as createdate,
         | cast(big.fprojectname as varchar(1000)) as projectname,
         | cast(dp.fnumber as varchar(255)) as projectno ,
         | cast(dp.fcustomerorderid as varchar(128)) as customerorderid,
         | cast(big.F_PAEZ_TEXT1 as varchar(255)) as salescompany,
         | cast(big.F_PAEZ_TEXT2 as varchar(128)) as salesdept,
         | cast(du.fname as varchar(64)) as createuser,
         | cast(ds.fname as varchar(64)) as salename,
         | cast(de.fname as varchar(64)) as operatorname,
         | cast(dm.fnumber as varchar(128)) as materialno,
         | cast(dm.f_paez_text as varchar(255)) as brand,
         | cast(dm.fname as varchar(500)) as materialname,
         | cast(dm.fspecification as varchar(500)) as specification,
         | cast(oes.fqty as decimal(19,4)) as qty,
         | cast(oes.ftaxprice as decimal(19,4)) as taxprice,
         | cast(oes.fallamount as decimal(19,4)) as allamount,
         | cast(oes.f_paez_text as varchar(64)) as consignee,
         | cast(oes.f_paez_text1 as varchar(64))  as contactphone,
         | cast(oes.f_paez_text2 as varchar(500)) as shppingaddress,
         | cast(case when dc.f_paez_checkbox = 0 then '否'
         | 	  when dc.f_paez_checkbox = 1 then '是'
         | 	  else dc.f_paez_checkbox end as varchar(32)) as isinternalcustomer,
         | cast(case when oes.fpurtype = 'A' then '自采'
         | 	when oes.fpurtype = 'B' then '组织间采购'
         | 	else oes.fpurtype end as varchar(32)) as purtype,
         | cast(case when oes.fmrpterminatestatus = 'A' then '正常'
         | 	when oes.fmrpterminatestatus = 'B' then '业务终止'
         | 		else oes.fmrpterminatestatus end as varchar(32)) as mrpterminatestatus,
         | cast(case when oes.f_paez_checkbox = 1 then '是'
         | 	  when oes.f_paez_checkbox = 0 then '否'
         | 	  else oes.f_paez_checkbox end as varchar(32)) as isdx,
         | cast(case when (oes.f_paez_checkbox = 1 OR dp.fnumber like '%HZXM%') then "非自营"
         |	else "自营" end as varchar(32)) as performanceform,
         | cast(dun.fname as varchar(64)) as unitname,
         | cast(oes.fnote as varchar(1000)) as note,
         | cast(oes.fbillno as varchar(128)) saleorderno,
         | cast("小票销售订单" as varchar(32)) as bill_type,
         | cast(case when  oes.fdocumentstatus = 'Z' then '暂存'
         | 	when oes.fdocumentstatus = 'A' then '创建'
         | 	when oes.fdocumentstatus = 'B' then '审核中'
         | 	when oes.fdocumentstatus = 'C' then '已审核'
         | 	when oes.fdocumentstatus = 'D' then '重新审核'
         | 	else oes.fdocumentstatus end as varchar(32)) as documentstatus,
         | oesee.FSTOCKBASESTOCKOUTQTY,
         | org.fname SALEORGNAME
         |from ${TableName.DWD_SAL_ORDER} oes
         |left join ${TableName.DIM_PROJECTBASIC} dp on oes.fprojectbasic  = dp.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on dp.fnumber = big.fbillno
         |left join ${TableName.DIM_ORGANIZATIONS} org on oes.FSALEORGID = org.forgid
         |left join ${TableName.DIM_USER} du on oes.FCREATORID = du.fuserid
         |left join ${TableName.DIM_SALEMAN} ds on oes.fsalerid = ds.fid
         |left join ${TableName.DIM_EMPINFO} de on oes.f_paez_base3 = de.fid
         |left join ${TableName.DIM_UNIT} dun on oes.funitid = dun.funitid
         |left join ${TableName.DIM_MATERIAL} dm on oes.fmaterialid = dm.fmaterialid
         |left join ${TableName.DIM_CUSTOMER} dc on oes.fcustid = dc.fcustid
         |left join ${TableName.ODS_ERP_SALORDERENTRY_E} oesee on oes.fentryid = oesee.fentryid
         |where COALESCE(oes.FORDERTYPE,0) <> 1
         |""".stripMargin)
    println(res.count())

    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_sale_performance"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res.write.mode("overwrite")
      .option("createTableColumnTypes", "createdate varchar(32),projectname varchar(1000),projectno varchar(255),customerorderid varchar(1500),salescompany varchar(255),salesdept varchar(128),createuser varchar(64),salename varchar(64),operatorname varchar(64),materialno varchar(128),brand varchar(255),materialname varchar(500),specification varchar(500),qty decimal(19,4),taxprice decimal(19,4),allamount decimal(19,4),consignee varchar(64),contactphone varchar(64),shppingaddress varchar(500),isinternalcustomer varchar(32),purtype varchar(32),mrpterminatestatus varchar(32),isdx varchar(32),performanceform varchar(32) ,unitname varchar(64),note varchar(1000),saleorderno varchar(128),bill_type varchar(32),documentstatus varchar(32),fstockbasestockoutqty INT,SALEORGNAME varchar(256)")// 明确指定 MySQL 数据库中字段的数据类型
      .option("batchsize", "10000")
      .option("truncate", "false")
      .option("jdbcType", s"createdate=${VARCHAR},projectname=${VARCHAR},projectno=${VARCHAR},salescompany=${VARCHAR},salesdept=${VARCHAR},createuser=${VARCHAR},salename=${VARCHAR},operatorname=${VARCHAR},materialno=${VARCHAR},brand=${VARCHAR},materialname=${VARCHAR}},specification=${VARCHAR}},qty=${DECIMAL},taxprice=${DECIMAL},allamount=${DECIMAL},consignee=${VARCHAR},contactphone=${VARCHAR},shppingaddress=${VARCHAR},isinternalcustomer=${VARCHAR},purtype=${VARCHAR},mrpterminatestatus=${VARCHAR},isdx=${VARCHAR},performanceform=${VARCHAR},unitname=${VARCHAR},note=${VARCHAR},saleorderno=${VARCHAR},bill_type=${VARCHAR},documentstatus=${VARCHAR},fstockbasestockoutqty =${INTEGER},SALEORGNAME=${VARCHAR}")   // 显式指定 SparkSQL 中的数据类型和 MySQL 中的映射关系
      .jdbc(url, table, props)
  }

}



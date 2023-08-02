package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{AddressAnalysis, Config, TableName}
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/26 15:32
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: SaleSignboard
 * @Description: TODO
 * @Version 1.0
 */
object SalePerformBoard {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job SalePerformBoard.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)

    //关闭SparkSession
    spark.stop()
}
  def runRES(spark: SparkSession)={
    /**
     * DC.F_PAEZ_CHECKBOX = 0是取非内部客户的订单,COALESCE(OER.F_ORDERTYPE,0) <> 1是过滤掉大票订单
     * 大小票应收单判断逻辑 ODS_ERP_RECEIVABLE应收单的订单类型F_ORDERTYPE数值为1 大票订单  2 小票订单  3供应链订单
     * FPRICE不含税单价
     * FCOSTAMTSUM成本去税总额
     */
    spark.udf.register("address_get",(str:String) =>addressget(str))
    val res = spark.sql(
      s"""
        |SELECT DS.FNAME AS SALENAME
        |	,OER.F_PAEZ_TEXT22	AS  SALECOMPANY
        |	,OER.F_PAEZ_TEXT221 AS SALEDEPT
        |	,SUBSTRING(OER.FDATE,1,10) AS BUSINESSDATE
        |	,address_get(ifnull(OES.F_PAEZ_TEXT2,'a')) as salearea
        |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
        |		ELSE '自营' END AS PERFORMANCEFORM
        |	,CASE WHEN DWP.IS_DSHYW = '是' THEN '电商化业务'
        |		ELSE '非电商化业务' END AS IS_DSHYW
        |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
        |		ELSE '其他' END AS PROJECTSHORTNAME
        |	,DWC.COMPANYSHORTNAME
        |	,CAST(SUM(OERE.FPRICEQTY * OERE.FPRICE) AS DECIMAL(19,2)) AS SALEAMOUNT
        | ,CAST(SUM(OERE.FPRICEQTY * OERE.FTAXPRICE) AS DECIMAL(19,2)) AS SALETAXAMOUNT
        | ,OERE.F_PXDF_TEXT PROJECTNO,
        | CAST(sum(OERE.FCOSTAMTSUM) AS DECIMAL(19,2)) FCOSTAMTSUM
        |FROM ${TableName.ODS_ERP_RECEIVABLE} OER
        |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE ON OER.FID = OERE.FID
        |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FCUSTOMERID = DC.FCUSTID
        |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.F_PXDF_TEXT = DP.FNUMBER
        |LEFT JOIN ${TableName.ODS_ERP_SALORDER} OES ON IF(OERE.F_PAEZ_Text='',0,OERE.F_PAEZ_Text) = OES.FBILLNO
        |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
        |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OER.FSALEERID = DS.FID
        |LEFT JOIN ${TableName.DWD_WRITE_COMPANYNAME} DWC ON DWC.COMPANYNAME = OER.F_PAEZ_TEXT22
        |WHERE DC.F_PAEZ_CHECKBOX = 0 AND COALESCE(OER.F_ORDERTYPE,0) <> 1 AND OER.FDOCUMENTSTATUS = 'C'
        |GROUP BY DS.FNAME
        |	,OER.F_PAEZ_TEXT22
        |	,OER.F_PAEZ_TEXT221
        |	,SUBSTRING(OER.FDATE,1,10)
        |	,OES.F_PAEZ_TEXT2
        |	,CASE WHEN (OES.F_PAEZ_CHECKBOX = 1 OR OERE.F_PXDF_TEXT LIKE '%HZXM%') THEN '非自营'
        |		ELSE '自营' END
        |	,CASE WHEN DWP.IS_DSHYW = '是' THEN '电商化业务'
        |		ELSE '非电商化业务' END
        |	,CASE WHEN DWP.PROJECTSHORTNAME IS NOT NULL THEN DWP.PROJECTSHORTNAME
        |		ELSE '其他' END
        |	,DWC.COMPANYSHORTNAME
        | ,OERE.F_PXDF_TEXT
        |""".stripMargin)



    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_sale_performanceboard"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res.write.mode("overwrite").jdbc(url, table, props)

  }

  def addressget(infoadd:String) = {
    AddressAnalysis.provincesMatch(infoadd)
  }

}

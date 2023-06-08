package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.TableName
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/6/7 10:11
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: Dw_table
 * @Description: TODO
 * @Version 1.0
 */
object DwTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job UpdateData.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession)={
    import spark.implicits._
    //销售订单明细表 dwd_sale_orderentry
    spark.sql(
      s"""
        |INSERT OVERWRITE TABLE ${TableName.DWD_SALE_ORDERENTRY}
        |select
        |oes.FID,
        |oes.F_PAEZ_BASE,
        |oes.F_PAEZ_BASE1,
        |oes.FMATERIALID,
        |oes.FQTY,
        |oesf.FPRICEUNITID,
        |oesf.FPRICE,
        |oesf.FTAXPRICE,
        |oesf.FAMOUNT,
        |oesf.FALLAMOUNT,
        |OES.FMRPTERMINATESTATUS
        |from ${TableName.ODS_ERP_SALORDERENTRY} oes
        |left join ${TableName.ODS_ERP_SALORDERENTRY_F} oesf on oesf.fentryid = oes.fentryid
        |""".stripMargin)

    //销售订单 dwd_sale_order
    spark.sql(
      s"""
        |INSERT OVERWRITE TABLE ${TableName.DWD_SALE_ORDER}
        |select
        |FID,
        |FCREATOR,
        |FSALERID,
        |F_PAEZ_BASE3 AS FOPERATORID,
        |FBILLTYPEID,
        |FBILLNO,
        |FDOCUMENTSTATUS,
        |FCUSTID,
        |FSALEDEPTID,
        |FAPPROVEDATE,
        |FCLOSESTATUS,
        |F_PXDF_TEXT,
        |F_PAEZ_TEXT AS FCONSIGNEE,
        |F_PAEZ_TEXT1 AS FCONTACTPHONE,
        |F_PAEZ_TEXT2 AS FSHPPINGADDRESS,
        |FNOTE,
        |FCREATEDATE,
        |FMODIFYDATE,
        |F_PAEZ_TEXT13 AS FSALESCOMPANY,
        |F_PAEZ_TEXT14 AS FSALESDEPT,
        |FPROJECTBASIC,
        |FPURTYPE
        |from ${TableName.ODS_ERP_SALORDER}
        |""".stripMargin)

    //--大票项目单 dwd_sale_bigticketproject
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWD_SALE_BIGTICKETPROJECT}
         |select
         |FID
         |,FBILLNO
         |,FDOCUMENTSTATUS
         |,FSHIPPER
         |,FPROJECTNAME
         |,FCUSTOMERORDERID
         |,FDEPARTID
         |,FSALESMAN
         |,FFOLLOWMAN
         |,F_PAEZ_TEXT1 AS FSALESCOMPANY
         |,F_PAEZ_TEXT2 AS FSALESDEPT
         |,FCREATEDATE
         |,FMODIFYDATE
         |from ${TableName.ODS_ERP_BIGTICKETPROJECT}
         |""".stripMargin)

  }
}

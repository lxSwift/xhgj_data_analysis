package com.xhgj.bigdata.firstProject

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.xhgj.bigdata.util.TableName
/**
 * @Author luoxin
 * @Date 2023/5/31 8:39
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: UpdateData
 * @Description: 完成hive表的增量更新要求
 * @Version 1.0
 */
object UpdateData {
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

  /**
   *如果需要设置写入的文件数, 需要做以下操作
   *      val df = spark.sql("SELECT * FROM table_name") // 读取表数据
   *      val repartitionedDF = df.repartition(N) // 将数据分成N个分区
   *      repartitionedDF.write.mode("overwrite").insertInto("table_name") // 写入数据
   *
   * @param spark
   */
  def runRES(spark: SparkSession): Unit = {

    //
    //ODS_ERP_RECEIVABLE增量更新
    spark.sql(
      s"""
        |SELECT
        | *,
        | row_number() over(partition by FID order by FMODIFYDATE desc) rank
        |FROM
        |${TableName.ODS_ERP_RECEIVABLE}
        |""".stripMargin).createOrReplaceTempView("RECEIVABLE")

    spark.sql(
      s"""
        |insert overwrite table ${TableName.ODS_ERP_RECEIVABLE}
        |select
        | FID,
        | FBILLTYPEID,
        | FBILLNO,
        | FDATE,
        | FENDDATE,
        | FCUSTOMERID,
        | FCURRENCYID,
        | FSETTLEORGID,
        | FSALEORGID,
        | FOWNERTYPE,
        | FOWNERID,
        | FALLAMOUNTFOR,
        | FWRITTENOFFSTATUS,
        | FOPENSTATUS,
        | FWRITTENOFFAMOUNTFOR,
        | FNOTWRITTENOFFAMOUNTFOR,
        | FRELATEHADPAYAMOUNT,
        | FWRITTENOFFAMOUNT,
        | FNOTWRITTENOFFAMOUNT,
        | FDOCUMENTSTATUS,
        | FCREATORID,
        | FCREATEDATE,
        | FMODIFIERID,
        | FMODIFYDATE,
        | FACCOUNTSYSTEM,
        | FISTAX,
        | FBYVERIFY,
        | FCANCELSTATUS,
        | FCANCELLERID,
        | FCANCELDATE,
        | FAPPROVERID,
        | FAPPROVEDATE,
        | FPAYCONDITON,
        | FSALEDEPTID,
        | FSALEGROUPID,
        | FSALEERID,
        | FCREDITCHECKRESULT,
        | FISINIT,
        | FSALESBUSTYPE,
        | FBUSINESSTYPE,
        | FPAYORGID,
        | FISRETAIL,
        | FCASHSALE,
        | FMATCHMETHODID,
        | FISB2C,
        | FISPRICEEXCLUDETAX,
        | FSETACCOUNTTYPE,
        | FISHOOKMATCH,
        | FISWRITEOFF,
        | FISINVOICEARLIER,
        | FREDBLUE,
        | FREMARK,
        | F_PAEZ_TEXT3,
        | F_PAEZ_TEXT4,
        | F_PAEZ_TEXT5,
        | FBILLMATCHLOGID,
        | F_ISCREATEEASVOUCHER,
        | F_ORDERTYPE,
        | F_PXDF_TEXT41,
        | F_PXDF_TEXT411,
        | F_PXDF_TEXT42,
        | F_PXDF_TEXT43,
        | F_PXDF_TEXT4121,
        | F_PXDF_TEXT413,
        | F_PXDF_TEXT4,
        | F_PXDF_DATE,
        | F_PXDF_TEXT5,
        | F_PXDF_TEXT6,
        | F_PXDF_PRINTTIMES,
        | F_PXDF_REMARKS,
        | F_PXDF_TEXT811,
        | FFPLX,
        | FSYB,
        | F_PXDF_TEXT7,
        | F_PXDF_TEXT8,
        | F_PXDF_TEXT9,
        | F_PXDF_TEXT10,
        | F_PXDF_TEXT11,
        | F_PXDF_REMARKS1,
        | F_PXDF_COMBO,
        | F_PXDF_CHECKBOX,
        | F_PXDF_COMBO1,
        | FBRANCHID,
        | F_PAEZ_ATTACHMENTCOUNT,
        | F_PAEZ_TEXT10,
        | F_PXDF_COMBO2,
        | F_PAEZ_BASE4,
        | FXMGLPROJECTNO,
        | FISBILLCHANGE,
        | F_PAEZ_TEXT7,
        | F_PAEZ_CHECKBOX2,
        | F_PAEZ_CHECKBOX3,
        | FPRESETBASE1,
        | FPRESETBASE2,
        | FPRESETASSISTANT1,
        | FPRESETASSISTANT2,
        | FPRESETTEXT1,
        | FPRESETTEXT2,
        | FORDERDISCOUNTAMOUNTFOR,
        | F_PAEZ_BASE31,
        | F_PAEZ_TEXT16,
        | F_PAEZ_TEXT17,
        | F_PAEZ_REMARKS,
        | F_PAEZ_TEXT22,
        | F_PAEZ_TEXT221,
        | F_PAEZ_BASE7,
        | F_PAEZ_BASE71,
        | F_PAEZ_AMOUNT4,
        | F_PAEZ_BASE8,
        | FFPDM,
        | F_PAEZ_TEXT13,
        | F_PAEZ_TEXT14,
        | FISGENERATEPLANBYCOSTITEM,
        | F_PAEZ_AMOUNT41,
        | F_PAEZ__PAEZ_CHECKBOX,
        | F_PAEZ__PXDF_TEXT9,
        | F_PAEZ_BASE9,
        | FFPCKLX
        |from RECEIVABLE
        |where
        |rank = 1
        |""".stripMargin)
  }
}

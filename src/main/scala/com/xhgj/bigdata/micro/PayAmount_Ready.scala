package com.xhgj.bigdata.micro

import com.xhgj.bigdata.util.TableName
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/6/13 14:46
 * @PackageName:com.xhgj.bigdata.micro
 * @ClassName: PayAmount_Ready
 * @Description: 基础表建立
 * @Version 1.0
 */
object PayAmount_Ready {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job PayAmount_Ready.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession) = {
    //应收单相关信息表更新
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWS_RECE_PAYAMOUNT}
         |select
         |	pro.fname PRONAME,
         |	pro.fnumber PRONO,
         |	cus.Fname CUSTNAME,
         |	cus.fnumber fnumber,
         |	sal.fname salename,
         |	sal.fnumber fnumber,
         |	a.F_PXDF_TEXT43 FaPiaoNo,
         |	a.F_PXDF_DATE FaPiaoDate,
         |	a.FALLAMOUNTFOR ALLAMOUNTFOR
         |from
         |	ODS_XHGJ.ODS_ERP_RECEIVABLE a
         |join ODS_XHGJ.ODS_ERP_RECEIVABLEENTRY b on a.fid=b.fid
         |join DW_XHGJ.DIM_PROJECTBASIC pro on b.FPROJECTNO=pro.fid
         |left join DW_XHGJ.DIM_CUSTOMER cus on cus.fcustid=a.FCUSTOMERID
         |left join DW_XHGJ.DIM_SALEMAN sal on sal.fid=a.FSALEERID
         |where a.FDOCUMENTSTATUS='C'
         |""".stripMargin)

    //收款单相关信息更新
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWS_RECE_PAYAMOUNTGET}
         |select
         |	a.FBILLNO FBILLNO,
         |	a.FDATE FDATE,
         |	a.FREALRECAMOUNTFOR FREALRECAMOUNTFOR,
         |	sety.fname FSRCSETTLETYPEID,
         |	pro.fnumber FPROJECTNO,
         |	pro.fname PRONAME,
         |	cus.fnumber fnumber_cus,
         |	cus.Fname CUSTNAME,
         |	sal.fnumber fnumber_sal,
         |	sal.fname salename
         |from
         |	ODS_XHGJ.ODS_ERP_RECEIVEBILL a
         |left join ODS_XHGJ.ODS_ERP_RECEIVEBILLENTRY b on b.FID=a.FID
         |left join DW_XHGJ.DIM_PROJECTBASIC pro on b.FPROJECTNO=pro.fid
         |left join DW_XHGJ.DIM_CUSTOMER cus on a.FCONTACTUNIT = fcustid
         |left join DW_XHGJ.DIM_SALEMAN sal on sal.fid=a.FSALEERID
         |left join DW_XHGJ.DIM_SETTLETYPE sety on sety.fid = b.FSETTLETYPEID
         |""".stripMargin)

  }
}

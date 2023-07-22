package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.{Config, MysqlConnect, PathUtil, TableName}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ceshi {
  val takeRow = 20

  case class Params(inputDay: String =null)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job ceshi.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }


  def runRES(spark: SparkSession)= {
    //应付单列表, 主表,单据状态只取已审核的数据以及结算组织为万聚的


        spark.sql(
          s"""
             |select staffname,deptname from (
             |select ds.fname staffname,
             |	dd.fname deptname,
             |	row_number() over(partition by ds.fname order by dd.fcreatedate desc) rn
             |from ${TableName.DIM_STAFF} ds
             |left join dw_xhgj.dim_department dd on ds.fdeptid = dd.fdeptid
             |where ds.FUSEORGID = 1
             |) dsdd where rn = 1
             |""".stripMargin).createOrReplaceTempView("staff")
    spark.sql(
      s"""
         |select
         | 	oep.fbillno,
         |	oep.fcreatedate,
         |	oep.fapprovedate,
         |	oepe.fmaterialid,
         |	oepr.fsrcbillno,
         |	case when oep.fdocumentstatus = 'Z' then '暂存'
         | 		 when oep.fdocumentstatus = 'A' then '创建'
         | 		 when oep.fdocumentstatus = 'B' then '审核中'
         | 		 when oep.fdocumentstatus = 'C' then '已审核'
         | 		 when oep.fdocumentstatus = 'D' then '重新审核'
         | 		 else oep.fdocumentstatus end as fdocumentstatus,
         |	ds.fname suppliername ,
         |	staff.deptname purdeptnmea,
         |	staff.staffname purchasername,
         |	oepl.fsbillid,
         |	oepl.fsid,
         |	oep.fid,
         |	oepe.fentryid,
         |	if(oepe.FQTY='',null,oepe.FQTY) as purqty
         |from ods_xhgj.ODS_ERP_POORDER oep
         |left join ods_xhgj.ODS_ERP_POORDERENTRY oepe on oep.fid = oepe.fid
         |left join ods_xhgj.ODS_ERP_POORDERENTRY_R oepr on oepe.fentryid = oepr.fentryid
         |left join ods_xhgj.ODS_ERP_POORDERENTRY_LK oepl on oepl.fentryid = oepe.fentryid
         |LEFT JOIN dw_xhgj.dim_user du on oep.fcreatorid = du.fuserid
         |left join staff on staff.staffname = du.fname
         |LEFT JOIN dw_xhgj.DIM_SUPPLIER ds ON oep.fsupplierid = ds.fsupplierid
         | WHERE oep.FBILLNO <> '' and oepr.FSRCBILLNO <> '' and oep.fpurchaseorgid = '1'
         |""".stripMargin).createOrReplaceTempView("a2")

    spark.sql(
      s"""
         |SELECT oei.fbillno,
         |	oei.fcreatedate,
         |	oei.fapprovedate,
         |	oeie.fmaterialid,
         |	oeie.fsrcbillno,
         |	dl.fname flotname,
         |	ds.fname stockname,
         |	oeil.fsbillid,
         |	oeil.fsid,
         |	if(oeie.FREALQTY='',null,oeie.FREALQTY) as instockqty
         |FROM ods_xhgj.ODS_ERP_INSTOCK oei
         |left join ods_xhgj.ODS_ERP_INSTOCKENTRY oeie on oei.fid = oeie.fid
         |left join ods_xhgj.ODS_ERP_INSTOCKENTRY_LK oeil on oeie.fentryid = oeil.fentryid
         |left join dw_xhgj.DIM_LOTMASTER dl on dl.flotid = oeie.flot
         |left join dw_xhgj.dim_stock ds on  ds.fstockid = oeie.FSTOCKID
         |WHERE oei.FBILLNO <> '' and oeie.FSRCBILLNO <>'' and oei.FSTOCKORGID = '1'
         |""".stripMargin).createOrReplaceTempView("a3")

    val result = spark.sql(
      s"""
         |select
         | 	a2.fbillno,
         |	a2.fcreatedate as purcreadate,
         |	a2.fapprovedate as purappdate,
         |	a2.fdocumentstatus,
         |	a2.suppliername ,
         |	a2.purdeptnmea,
         |	a2.purchasername,
         |	a2.fsbillid,
         |	a2.fsid,
         |	a2.fid,
         |	a2.fentryid,
         |	a3.fbillno as purno,
         |	a3.fcreatedate as increadate,
         |	a3.fapprovedate  as inappdate,
         |	a3.flotname,
         |	a3.stockname,
         |	COALESCE (a3.instockqty,a2.purqty)*1 as purqty,
         |	a3.instockqty
         |from a2
         |left join a3 on a3.fsbillid = a2.fid and a3.fsid = a2.fentryid
         |where  a2.fbillno = 'WJ00003607'
         |""".stripMargin)
        println("result="+result.count())
        result.show(100)

  }
}

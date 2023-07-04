package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{Config, TableName}
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/16 16:13
 * @PackageName:com.xhgj.bigdata.textProject
 * @ClassName: ResTest
 * @Description: TODO
 * @Version 1.0
 */
object ProcureMonitor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job ProcureMonitor.scala")
      .enableHiveSupport()
      .getOrCreate()

//    runRES(spark)
    takeTime(spark)
    purAmount(spark)
    RerurnStock(spark)
    WorkLoad(spark)

    //关闭SparkSession
    spark.stop()
  }

  /**
   * 采购过程--各个阶段花费的时间
   * @param spark
   */
  def takeTime(spark: SparkSession): Unit = {

    /**
     * 将采购申请单表以及其衍生表和维度表关联起来, 并且只需要申请组织为1即万聚公司的值(这个是去组织维度表查到的对应)
     */
    spark.sql(
      s"""
         |select oer.fbillno,
         | 	oer.fcreatedate,
         | 	oer.fapprovedate,
         | 	oere.fmaterialid,
         | 	case when oer.fdocumentstatus = 'Z' then '暂存'
         | 		 when oer.fdocumentstatus = 'A' then '创建'
         | 		 when oer.fdocumentstatus = 'B' then '审核中'
         | 		 when oer.fdocumentstatus = 'C' then '已审核'
         | 		 when oer.fdocumentstatus = 'D' then '重新审核'
         | 	 		 else oer.fdocumentstatus end as fdocumentstatus,
         | 	 	do1.fname reqorgname,
         | 	 	oer.fapplicationdate ,
         | 	 	dp.fname f_projectno,
         | 	 	dm.fnumber materno,
         | 	 	dm.fname matername,
         | 	 	dm.f_paez_text brandname,
         | 	 	dm.fspecification specification,
         | 		do2.fname saleorgname,
         | 		ds.fname saler ,
         | 		oerr.fsrcbillno,
         | 		oer.fid,
         | 		oere.fentryid,
         | 		oerl.fsbillid ,
         | 		oerl.fsid,
         | 		DC.FNAME KHLB,
         | 		oere.FAPPROVEQTY as reqqty
         | from ${TableName.ODS_ERP_REQUISITION} oer
         | left join ${TableName.ODS_ERP_REQENTRY} oere on oer.fid = oere.fid
         | left join ${TableName.ODS_ERP_REQENTRY_R} oerr on oere.fentryid = oerr.fentryid
         | left join ${TableName.ODS_ERP_REQENTRY_LK} oerl on oere.fentryid = oerl.fentryid
         | LEFT JOIN ${TableName.DIM_MATERIAL} dm on oere.fmaterialid = dm.fmaterialid --物料表
         | LEFT JOIN ${TableName.DIM_CUST100501} DC ON dm.f_khr = DC.FID
         | LEFT JOIN ${TableName.DIM_ORGANIZATIONS} do1 on oer.FAPPLICATIONORGID = do1.forgid
         | LEFT JOIN ${TableName.DIM_ORGANIZATIONS} do2 on oer.f_pxdf_orgid = do2.forgid
         | LEFT JOIN ${TableName.DIM_SALEMAN} ds on oere.f_paez_base1 = ds.fid
         | LEFT JOIN ${TableName.DIM_PROJECTBASIC} dp ON oere.f_projectno = DP.fid
         | WHERE oer.FAPPLICATIONORGID = '1'
         |""".stripMargin).createOrReplaceTempView("a1")

    /**
     * 采购订单筛选出万聚的以及单号源单号不为空的数据
     */
    spark.sql(
      s"""
        |select oep.fbillno,
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
        |	dd.fname purdeptnmea,
        |	db.fname purchasername,
        |	oepl.fsbillid,
        |	oepl.fsid,
        |	oep.fid,
        |	oepe.fentryid,
        |	oepe.FQTY as purqty
        |from ${TableName.ODS_ERP_POORDER} oep
        |left join ${TableName.ODS_ERP_POORDERENTRY} oepe on oep.fid = oepe.fid
        |left join ${TableName.ODS_ERP_POORDERENTRY_R} oepr on oepe.fentryid = oepr.fentryid
        |left join ${TableName.ODS_ERP_POORDERENTRY_LK} oepl on oepl.fentryid = oepe.fentryid
        |LEFT JOIN ${TableName.DIM_DEPARTMENT} dd on oep.fpurchasedeptid = dd.fdeptid
        |LEFT JOIN ${TableName.DIM_BUYER} db on oep.fpurchaserid = db.fid
        |LEFT JOIN ${TableName.DIM_SUPPLIER} ds ON oep.fsupplierid = ds.fsupplierid
        | WHERE oep.FBILLNO <> '' and oepr.FSRCBILLNO <> '' and oep.fpurchaseorgid = '1'
        |""".stripMargin).createOrReplaceTempView("a2")

    /**
     * 采购入库单获得收料组织为1万聚且单据以及源单编码不为空
     */
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
        |	oeie.FREALQTY as instockqty
        |FROM ${TableName.ODS_ERP_INSTOCK} oei
        |left join ${TableName.ODS_ERP_INSTOCKENTRY} oeie on oei.fid = oeie.fid
        |left join ${TableName.ODS_ERP_INSTOCKENTRY_LK} oeil on oeie.fentryid = oeil.fentryid
        |left join ${TableName.DIM_LOTMASTER} dl on oeie.flot = dl.flotid
        |left join ${TableName.DIM_STOCK} ds on oeie.FSTOCKID = ds.fstockid
        |WHERE oei.FBILLNO <> '' and oeie.FSRCBILLNO <>'' and oei.FSTOCKORGID = '1'
        |""".stripMargin).createOrReplaceTempView("a3")

    /**
     * 出库通知单获取批号
     *
     */
    spark.sql(
      s"""
         |select oed.fbillno,
         |	oed.fcreatedate,
         |	oed.fapprovedate,
         |	oede.fmaterialid,
         |	dl.fname flotname,
         |	sum(oede.fqty) over(partition by dl.fname,oede.fmaterialid) noticeqty,
         |	row_number() over(partition by dl.fname,oede.fmaterialid order by COALESCE(oed.fapprovedate,oed.fcreatedate) desc) as rn
         |from ${TableName.ODS_ERP_DELIVERYNOTICE} oed
         |left join ${TableName.ODS_ERP_DELIVERYNOTICEENTRY} oede on oed.fid = oede.fid
         |left join ${TableName.DIM_LOTMASTER} dl on oede.flot = dl.flotid
         |where COALESCE(oede.flot,'') != ''
         |""".stripMargin).createOrReplaceTempView("a4")

    spark.sql(
      s"""
         |select fbillno,
         |	fcreatedate,
         |	fapprovedate,
         |	fmaterialid,
         |	flotname,
         |	noticeqty
         |from a4
         |where rn = 1
         |""".stripMargin).createOrReplaceTempView("a5")

    /**
     * 出库单
     */
    spark.sql(
      s"""
        |select
        |  	oeo.fbillno,
        |	oeo.fcreatedate,
        |	oeo.fapprovedate,
        |	oeoe.fmaterialid,
        |	dl.fname as flotname,
        |	sum(oeoe.FREALQTY) over(partition by dl.fname,oeoe.fmaterialid) as outstockqty,
        |	row_number() over(partition by dl.fname,oeoe.fmaterialid order by COALESCE(oeo.fapprovedate,oeo.fcreatedate) desc) as rn
        |from ${TableName.ODS_ERP_OUTSTOCK} oeo
        |left join ${TableName.ODS_ERP_OUTSTOCKENTRY} oeoe on oeo.fid = oeoe.fid
        |left join ${TableName.ODS_ERP_OUTSTOCKENTRY_LK} oeol on oeoe.fentryid = oeol.fentryid
        |left join ${TableName.DIM_LOTMASTER} dl on oeoe.flot = dl.flotid
        |where COALESCE(oeoe.flot,'') != ''
        |""".stripMargin).createOrReplaceTempView("a6")

    spark.sql(
      s"""
         |select
         | 	fbillno,
         |	fcreatedate,
         |	fapprovedate,
         |	fmaterialid,
         |	flotname,
         |	outstockqty
         |from a6
         |where rn = 1
         |""".stripMargin).createOrReplaceTempView("a7")

    /**
     * 出库通知单和出库单之间通过源单, 通知单明细号, 物料号,进行关联
     * 并根据批料号物料号创建日期申请日期进行分组按照出库单的申请日期进行
     */
    spark.sql(
      s"""
         |select
         |	fbillno,
         |	fcreatedate,
         |	fapprovedate,
         |	fid,
         |	fentryid,
         |	fmaterialid,
         |	fqty as saleqty
         |from ${TableName.DWD_SAL_ORDER} dso
         |where fbillno <> ''
         |""".stripMargin).createOrReplaceTempView("a8")



    val res = spark.sql(
      s"""
         |select a1.fbillno as reqno,
         | 	a1.fdocumentstatus as reqstatus,
         | 	a1.reqorgname,
         | 	a1.fapplicationdate as reqapplidate,
         | 	a1.f_projectno as reqprono,
         | 	a1.saleorgname,
         | 	a1.saler,
         | 	a8.fbillno as saleno,
         | 	a2.fbillno as purno,
         | 	a2.fdocumentstatus as purstatus,
         | 	a2.suppliername,
         | 	a2.purdeptnmea,
         | 	a2.purchasername,
         | 	a1.materno,
         | 	a1.matername,
         | 	a1.brandname,
         | 	a1.specification,
         | 	a1.khlb testtype,
         | 	a8.fcreatedate as salecreadate,
         | 	a8.fapprovedate as saleappdate,
         | 	a1.fcreatedate as reqcreadate,
         | 	a1.fapprovedate as reqappdate,
         | 	a2.fcreatedate as purcreadate,
         | 	a2.fapprovedate as purappdate,
         | 	a3.fcreatedate as increadate,
         | 	a3.fapprovedate as inappdate,
         | 	a5.fcreatedate as sendcreadate,
         | 	a5.fapprovedate as sendappdate,
         | 	a7.fcreatedate as outcreadate,
         | 	a7.fapprovedate as outappdate,
         | 	datediff(from_unixtime(unix_timestamp(a8.fapprovedate),'yyyy-MM-dd'),from_unixtime(unix_timestamp(a8.fcreatedate),'yyyy-MM-dd')) as salfordate,
         | 	datediff(from_unixtime(unix_timestamp(a1.fapplicationdate),'yyyy-MM-dd'),from_unixtime(unix_timestamp(a1.fcreatedate),'yyyy-MM-dd')) as reqfordate,
         | 	datediff(from_unixtime(unix_timestamp(a2.fapprovedate),'yyyy-MM-dd'),from_unixtime(unix_timestamp(a1.fapprovedate),'yyyy-MM-dd')) as poofordate,
         | 	a1.reqqty,
         | 	a2.purqty,
         | 	a3.fbillno as instockno,
         | 	a3.flotname as instockflot,
         | 	a3.stockname as instock,
         | 	a3.instockqty,
         |	a5.fbillno as noticeno,
         | 	a5.noticeqty,
         | 	a7.fbillno as outstockno,
         | 	a8.saleqty,
         |  a7.outstockqty
         | from a1 left join a2 on a1.fid = a2.fsbillid
         | AND A1.fentryid = A2.fsid
         | left join a3 on a2.fid = a3.fsbillid
         | AND A2.fentryid = a3.fsid
         | left join a5 on a5.flotname = a3.flotname and a5.fmaterialid = a3.fmaterialid
         | left join a7 on a7.flotname = a3.flotname and a7.fmaterialid = a3.fmaterialid
         | left join a8 on a1.fsid = a8.fentryid and a1.fsbillid = a8.fid
         |""".stripMargin)
    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_pur_taketime"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res.write.mode("overwrite").jdbc(url, table, props)

  }

  /**
   *采购过程--金额取数
   * @param spark
   */
  def purAmount(spark: SparkSession) = {
    //已开票金额
    spark.sql(
      s"""
         |SELECT	--已开票金额
         |	substring(oer.fdate,1,10) fdate,
         |	oere.F_PAEZ_BASE purdept,
         |	oere.F_PAEZ_BASE1 purperson,
         |	oere.fmaterialid,
         |	SUM(oere.FPRICEQTY * OERE.FTAXPRICE) AS FAMOUNT
         |FROM ${TableName.ODS_ERP_RECEIVABLE} oer
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} oere ON OER.fid = oere.fid
         |WHERE oer.FSETTLEORGID = 1 and FDOCUMENTSTATUS = 'C'
         |GROUP BY
         |oere.F_PAEZ_BASE ,
         |	oere.F_PAEZ_BASE1 ,
         |	oere.fmaterialid,
         |	substring(oer.fdate,1,10)
         |""".stripMargin).createOrReplaceTempView("a1")
    //--未开票金额
    spark.sql(
      s"""
         |SELECT fdate,PURDEPT,PURPERSON,fmaterialid,SUM(FAMOUNT) FAMOUNT FROM (
         |select 	--未开票金额
         |substring(oea.fdate,1,10) fdate,
         |oead.F_PXDF_BASE2 purdept,
         |oead.F_PXDF_BASE1 purperson,
         |oead.fmaterialid,
         |oead.F_PXDF_QTY * oead.FTAXPRICE AS FAMOUNT
         |from ${TableName.ODS_ERP_ARSETTLEMENT} oea
         |left join ${TableName.ODS_ERP_ARSETTLEMENTDETAIL} oead on oea.fid = oead.fid
         |where oea.FACCTORGID = 1
         |UNION ALL
         |select --未开票金额
         |substring(oeo.fdate,1,10) fdate,
         |oeoe.F_PAEZ_BASE2 purdept,
         |oeoe.F_PAEZ_BASE1 pyrperson,
         |oeoe.fmaterialid,
         |oeor.FARNOTJOINQTY*oeof.FTAXPRICE AS FAOMUNT
         |from ${TableName.ODS_ERP_OUTSTOCK} oeo
         |left join ${TableName.ODS_ERP_OUTSTOCKENTRY} oeoe on oeo.fid = oeoe.fid
         |left join ${TableName.ODS_ERP_OUTSTOCKENTRY_F} oeof on oeoe.fentryid = oeof.fentryid
         |left join ${TableName.ODS_ERP_OUTSTOCKENTRY_R} oeor on oeoe.fentryid = oeor.fentryid
         |where oeo.FSTOCKORGID = 1
         |) WEI
         |GROUP BY fdate,PURDEPT,PURPERSON,fmaterialid
         |""".stripMargin).createOrReplaceTempView("a2")

    //在执行金额
    spark.sql(
      s"""
         |SELECT --在执行金额
         |substring(oep.fdate,1,10) fdate,
         |oep.FPURCHASEDEPTID purdept,
         |oep.FPURCHASERID purperson,
         |oepe.fmaterialid,
         |sum(oepe.F_PAEZ_AMOUNT) AS FAMOUNT
         |FROM ${TableName.ODS_ERP_POORDER} oep
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY} oepe on oep.fid =oepe.fid
         |where FPURCHASEORGID = 1 and oepe.FMRPCLOSESTATUS = 'A'
         |group by
         |substring(oep.fdate,1,10),
         |oep.FPURCHASEDEPTID,
         |oep.FPURCHASERID,
         |oepe.fmaterialid
         |""".stripMargin).createOrReplaceTempView("a3")

    spark.sql(
      s"""
         |SELECT
         |substring(oei.fdate,1,10) fdate,
         |oei.FPURCHASEDEPTID purdept,
         |oei.FPURCHASERID purperson,
         |oeie.fmaterialid,
         |oeif.F_PAEZ_AMOUNT famount
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie on oei.fid = oeie.fid
         |left join ${TableName.ODS_ERP_INSTOCKENTRY_F} oeif on oeie.fentryid = oeif.fentryid
         |left join ${TableName.DIM_STOCK} ds on oeie.FSTOCKID = ds.FSTOCKID
         |where oeie.FGIVEAWAY = 0 and ds.fname not like '%样品%' and oei.FSTOCKORGID = 1
         |""".stripMargin).createOrReplaceTempView("a4")

    spark.sql(
      s"""
         |SELECT
         |	nvl(nvl(nvl(a1.fdate,a2.fdate),a3.fdate),a4.fdate) fdate,
         |	nvl(nvl(nvl(a1.purdept,a2.purdept),a3.purdept),a4.purdept) purdept,
         |	nvl(nvl(nvl(a1.purperson,a2.purperson),a3.purperson),a4.purperson) purperson,
         |	nvl(nvl(nvl(a1.fmaterialid,a2.fmaterialid),a3.fmaterialid),a4.fmaterialid) fmaterialid,
         |	cast(nvl(a1.famount,0) as decimal(18,4)) ykpamount,
         |	cast(nvl(a2.famount,0) as decimal(18,4)) wkpamount,
         |	cast(nvl(a3.famount,0) as decimal(18,4)) zzxamount,
         |	cast(nvl(a4.famount,0) as decimal(18,4)) wdpamount
         |from a1
         |full join a2 on a1.purdept = a2.purdept and a1.purperson = a2.purperson and a1.fmaterialid = a2.fmaterialid and a1.fdate = a2.fdate
         |full join a3 on a2.purdept = a3.purdept and a2.purperson = a3.purperson and a2.fmaterialid = a3.fmaterialid and a2.fdate = a3.fdate
         |full join a4 on a4.purdept = a3.purdept and a4.purperson = a3.purperson and a4.fmaterialid = a3.fmaterialid and a4.fdate = a3.fdate
         |""".stripMargin).createOrReplaceTempView("a5")


    val res2 = spark.sql(
      s"""
         |SELECT A5.FDATE,
         |	dd.fname purdeptname,
         |	db.fname buyername,
         |	dm.fnumber materno,
         |	dm.fname matername,
         |	dm.f_paez_text brandname,
         |	dm.fspecification specification,
         |	a5.ykpamount ykpamount,
         |	a5.wkpamount wkpamount,
         |	a5.zzxamount zzxamount,
         |	a5.wdpamount wdpamount,
         |	dc.fname assesstype
         |FROM a5
         |LEFT JOIN ${TableName.DIM_MATERIAL} dm ON a5.fmaterialid = dm.fmaterialid
         |LEFT JOIN ${TableName.DIM_CUST100501} DC ON dm.f_khr = DC.FID
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} dd ON dd.fdeptid  = a5.PURDEPT
         |LEFT JOIN ${TableName.DIM_BUYER} db ON a5.PURPERSON = db.fid
         |WHERE a5.ykpamount <> 0 or a5.wkpamount <> 0 or a5.zzxamount <> 0 or a5.wdpamount <> 0
         |""".stripMargin)
    println("num==============" + res2.count())
    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_pur_amount"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res2.write.mode("overwrite").jdbc(url, table, props)
  }

  def runRes3(spark: SparkSession): Unit = {
    spark.sql(
      """
        |SELECT
        | namee
        |FROM
        |keetle_test.tes_tmp
        |group by namee
        |WHERE cast(idf as Double) <> '' or namee <> '' or cast(age as Double) <> '' or gender <> ''
        |""".stripMargin).show()
    println("=====================")
    spark.sql(
      """
        |SELECT
        | *
        |FROM
        |keetle_test.tes_tmp WHERE cast(idf as Double) <> 0 or namee <> 0 or cast(age as Double) <> 0 or gender <> 0
        |""".stripMargin).show()
  }
  def runRes4(spark: SparkSession): Unit = {
    spark.sql("""select oep.fbillno,
                |		oep.fcreatedate,
                |		oep.fapprovedate,
                |		oepe.fmaterialid,
                |		oepr.fsrcbillno,
                |		case when oep.fdocumentstatus = 'Z' then '暂存'
                |	 		 when oep.fdocumentstatus = 'A' then '创建'
                |	 		 when oep.fdocumentstatus = 'B' then '审核中'
                |	 		 when oep.fdocumentstatus = 'C' then '已审核'
                |	 		 when oep.fdocumentstatus = 'D' then '重新审核'
                |	 		 else oep.fdocumentstatus end as fdocumentstatus,
                |		ds.fname suppliername ,
                |		dd.fname purdeptnmea,
                |		db.fname purchasername,
                |		oepl.fsbillid,
                |		oepl.fsid,
                |		oep.fid,
                |		oepe.fentryid
                |	from ods_xhgj.ods_erp_poorder oep
                |	left join ods_xhgj.ods_erp_poorderentry oepe on oep.fid = oepe.fid
                |	left join ods_xhgj.ods_erp_poorderentry_r oepr on oepe.fentryid = oepr.fentryid
                |	left join ods_xhgj.ods_erp_poorderentry_lk oepl on oepl.fentryid = oepe.fentryid
                |	LEFT JOIN dw_xhgj.dim_department dd on oep.fpurchasedeptid = dd.fdeptid
                |	LEFT JOIN dw_xhgj.dim_buyer db on oep.fpurchaserid = db.fid
                |	LEFT JOIN dw_xhgj.dim_supplier ds ON oep.fsupplierid = ds.fsupplierid
                |	 WHERE oep.fbillno = 'WJ00347483' and oep.FBILLNO is not null and oepr.FSRCBILLNO is not null""".stripMargin).show(100)

  }

  /**
   * 采购过程--退货单
   * @param spark
   */
  def RerurnStock(spark: SparkSession): Unit = {
    val res = spark.sql(
      s"""
         |SELECT OER.FDATE,	--业务日期
         |	DD.FNAME DEPTNAME,	--新采购部门
         |	DB.FNAME BUYERNAME,--采购员
         |	DO.FNAME ORGNAME,	--销售组织
         |	DS.FNAME SALERNAME,--销售员
         |	OER.FBILLNO ,
         |	OERE.FMATERIALID,
         |	DM.FNAME MATERIALNAME--物料名称
         |FROM ${TableName.ODS_ERP_RETURNSTOCK} OER
         |LEFT JOIN ${TableName.ODS_ERP_RETURNSTOCKENTRY} OERE ON OER.FID  = OERE.FID
         |LEFT JOIN ${TableName.DIM_BUYER} DB ON OERE.F_PAEZ_BASE = DB.FID
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} DD ON DB.FDEPTID = DD.FDEPTID
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} DO ON OER.FSALEORGID = DO.FORGID
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON OER.FSALESMANID = DS.FID
         |LEFT JOIN ${TableName.DIM_MATERIAL} DM ON OERE.FMATERIALID = DM.FMATERIALID
         |WHERE FSTOCKORGID = '1'
         |""".stripMargin)
    println("num==============" + res.count())
    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_pur_returnstock"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res.write.mode("overwrite").jdbc(url, table, props)

  }

  /**
   * 采购过程--工作量
   * @param spark
   */
  def WorkLoad(spark: SparkSession)={
    spark.sql(
      s"""
         |SELECT FDATE,FPURCHASEDEPTID PURDEPTID,FPURCHASERID PURERID,COUNT(*) NUM
         |FROM (SELECT DISTINCT SUBSTRING(OER.FAPPLICATIONDATE,1,10) FDATE,OERS.FPURCHASEDEPTID,OERS.FPURCHASERID,OER.FBILLNO,OERE.FMATERIALID
         |FROM ${TableName.ODS_ERP_REQUISITION} OER
         |LEFT JOIN ${TableName.ODS_ERP_REQENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.ODS_ERP_REQENTRY_S} OERS ON OERE.FENTRYID = OERS.FENTRYID
         |WHERE OER.FAPPLICATIONORGID = '1'
         |) AAA
         |GROUP BY FDATE,FPURCHASEDEPTID,FPURCHASERID
         |""".stripMargin).createOrReplaceTempView("A1")

    spark.sql(
      s"""
         |SELECT SUBSTRING(FDATE,1,10) FDATE,OEP.FPURCHASEDEPTID PURDEPTID,OEP.FPURCHASERID PURERID,COUNT(*) NUM
         |FROM ${TableName.ODS_ERP_POORDER} OEP
         |WHERE FPURCHASEORGID = '1'
         |GROUP BY SUBSTRING(FDATE,1,10) ,OEP.FPURCHASEDEPTID,OEP.FPURCHASERID
         |""".stripMargin).createOrReplaceTempView("A2")

    spark.sql(
      s"""
         |SELECT FDATE,PURDEPTID,PURERID,PAYTYPEID,COUNT(*) NUM ,SUM(FPAYAMOUNTFOR_E) FAMOUNT FROM (
         |SELECT SUBSTRING(OEP.FDATE,1,10) FDATE, OEP.FPURCHASEDEPTID PURDEPTID,OEP.FPURCHASERID PURERID,OEPE.FSETTLETYPEID PAYTYPEID,OEP.FBILLNO,SUM(OEPE.FPAYAMOUNTFOR_E) FPAYAMOUNTFOR_E
         |FROM ${TableName.ODS_ERP_PAYBILL} OEP
         |LEFT JOIN ${TableName.ODS_ERP_PAYBILLENTRY} OEPE ON OEP.FID = OEPE.FID
         |WHERE  OEP.FPAYORGID = '1'
         |GROUP BY SUBSTRING(OEP.FDATE,1,10), OEP.FPURCHASEDEPTID,OEP.FPURCHASERID,OEPE.FSETTLETYPEID,OEP.FBILLNO
         |) BBB
         |GROUP BY FDATE,PURDEPTID,PURERID,PAYTYPEID
         |""".stripMargin).createOrReplaceTempView("A3")

    spark.sql(
      s"""
         |SELECT
         |COALESCE(A1.FDATE,A2.FDATE,A3.FDATE) FDATE,
         |COALESCE(A1.PURDEPTID,A2.PURDEPTID,A3.PURDEPTID) PURDEPTID,
         |COALESCE(A1.PURERID,A2.PURERID,A3.PURERID) PURERID,
         |A3.PAYTYPEID,
         |A1.NUM AS REQNUM,
         |A2.NUM AS PURNUM,
         |A2.NUM AS FKDNUM,
         |A3.FAMOUNT AS FKDAMOUNT
         |FROM A1
         |LEFT JOIN A2 ON A1.FDATE = A2.FDATE AND A1.PURDEPTID = A2.PURDEPTID AND A1.PURERID = A2.PURERID
         |LEFT JOIN A3 ON A3.FDATE = A2.FDATE AND A3.PURDEPTID = A2.PURDEPTID AND A3.PURERID = A2.PURERID
         |""".stripMargin).createOrReplaceTempView("A4")

    val res = spark.sql(
      s"""
         |SELECT A4.FDATE,
         |	DD.FNAME PURDEPTNAME,
         |	DB.FNAME BUYERNAME,
         |	DS.FNAME PAYTYPENAME,
         |	A4.REQNUM,
         |	A4.PURNUM,
         |	A4.FKDNUM,
         |	A4.FKDAMOUNT
         |FROM A4
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} DD ON A4.PURDEPTID = DD.FDEPTID
         |LEFT JOIN ${TableName.DIM_BUYER} DB ON A4.PURERID = DB.FID
         |LEFT JOIN ${TableName.DIM_SETTLETYPE} DS ON A4.PAYTYPEID = DS.FID
         |""".stripMargin)
    println("num==============" + res.count())

    // 定义 MySQL 的连接信息
    val conf = Config.load("config.properties")
    val url = conf.getProperty("database.url")
    val user = conf.getProperty("database.user")
    val password = conf.getProperty("database.password")
    val table = "ads_pur_workload"


    // 定义 JDBC 的相关配置信息
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "com.mysql.jdbc.Driver")

    // 将 DataFrame 中的数据保存到 MySQL 中(直接把原表删除, 建新表, 很暴力)
    res.write.mode("overwrite").jdbc(url, table, props)
  }
}

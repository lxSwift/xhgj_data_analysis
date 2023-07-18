package com.xhgj.bigdata.firstProject

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
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
    import spark.implicits._


    //ODS_ERP_INSTOCK表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FID order by FMODIFYDATE desc) rank
         |FROM
         |${TableName.ODS_ERP_INSTOCK}
         |""".stripMargin).createOrReplaceTempView("INSTOCK")

    spark.sql(
      s"""
         |insert overwrite table ${TableName.ODS_ERP_INSTOCK}
         |select
         |  FID,
         |	FOBJECTTYPEID,
         |	FBILLTYPEID,
         |	FBILLNO,
         |	FDATE,
         |	FSUPPLIERID,
         |	FSUPPLYID,
         |	FSETTLEID,
         |	FCHARGEID,
         |	FSTOCKORGID,
         |	FPURCHASEORGID,
         |	FPURCHASEDEPTID,
         |	FPURCHASERGROUPID,
         |	FPURCHASERID,
         |	FSTOCKERGROUPID,
         |	FSTOCKDEPTID,
         |	FSTOCKERID,
         |	FDELIVERYBILL,
         |	FTAKEDELIVERYBILL,
         |	FDOCUMENTSTATUS,
         |	FCANCELSTATUS,
         |	FCREATORID,
         |	FCREATEDATE,
         |	FMODIFIERID,
         |	FMODIFYDATE,
         |	FAPPROVERID,
         |	FAPPROVEDATE,
         |	FCANCELLERID,
         |	FCANCELDATE,
         |	FBUSINESSTYPE,
         |	FOWNERTYPEID,
         |	FOWNERID,
         |	FDEMANDORGID,
         |	FSUPPLYADDRESS,
         |	FAPSTATUS,
         |	FTRANSFERBIZTYPE,
         |	FCORRESPONDORGID,
         |	FISINTERLEGALPERSON,
         |	FSRCFID,
         |	FDISASSEMBLYFLAG,
         |	FCONFIRMSTATUS,
         |	FCONFIRMERID,
         |	FCONFIRMDATE,
         |	FPROVIDERCONTACTID,
         |	FBILLTYPEIDVM,
         |	F_PAEZ_TEXT1,
         |	FISSYNCHROTOWMS,
         |	F_PXDF_CHECKBOX,
         |	FPUSHFROMRETAILBILL,
         |	FSUPPLIERDELIVER,
         |	FORDERNO,
         |	FDELIVERYNO,
         |	FBRANCHID,
         |	FDELIVERYTIME,
         |	FDELIVERYCOMPANY,
         |	FDELIVERYSTOCK,
         |	FRECEIVERID,
         |	F_PAEZ_COMBO,
         |	F_PAEZ_BASE21,
         |	FXMGLPROJECTNO,
         |	FSPLITBILLTYPE,
         |	F_PAEZ_CHECKBOX,
         |	FSUPPLYEMAIL,
         |	FSALOUTSTOCKORGID,
         |	F_SLRQ,
         |	FLOGISTICSNO,
         |	FLOGISTICS,
         |	FISSOURCESRM,
         |	FISREJOUT,
         |	F_PAEZ_TEXT3,
         |	F_PAEZ__PAEZ_TEXT1
         |FROM
         |  INSTOCK
         |where
         |rank = 1
         |""".stripMargin)

    //ODS_ERP_INSTOCKENTRY表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FENTRYID order by F_TIMESTAMP desc) rank
         |FROM
         |${TableName.ODS_ERP_INSTOCKENTRY}
         |""".stripMargin).createOrReplaceTempView("INSTOCKENTRY")
    spark.sql(
      s"""
         |insert overwrite table ${TableName.ODS_ERP_INSTOCKENTRY}
         |select
         |  FENTRYID,
         |	FID,
         |	FSEQ,
         |	FMATERIALID,
         |	FUNITID,
         |	FAUXPROPID,
         |	FMUSTQTY,
         |	FREALQTY,
         |	FSTOCKID,
         |	FSTOCKLOCID,
         |	FSTOCKSTATUSID,
         |	FPRODUCEDATE,
         |	FEXPIRYDATE,
         |	FSHELFLIFE,
         |	FLOT,
         |	FLOT_TEXT,
         |	FSUPPLIERLOT,
         |	FBASEUNITID,
         |	FBASEUNITQTY,
         |	FAUXUNITID,
         |	FAUXUNITQTY,
         |	FBASEJOINQTY,
         |	FBOMID,
         |	FGROSSWEIGHT,
         |	FNETWEIGHT,
         |	FNOTE,
         |	FPOORDERNO,
         |	FCONTRACTNO,
         |	FREQTRACENO,
         |	FSRCBILLTYPEID,
         |	FSRCBILLNO,
         |	FSRCROWID,
         |	FSTOCKFLAG,
         |	FOWNERTYPEID,
         |	FOWNERID,
         |	FKEEPERTYPEID,
         |	FKEEPERID,
         |	FRECEIVESTOCKSTATUS,
         |	FRECEIVESTOCKFLAG,
         |	FRECEIVEOWNERTYPEID,
         |	FRECEIVEOWNERID,
         |	FBASEMUSTQTY,
         |	FRECEIVESTOCKID,
         |	FRECEIVESTOCKLOCID,
         |	FRECEIVELOT,
         |	FRECEIVELOT_TEXT,
         |	FRECEIVEAUXPROPID,
         |	FBASEAPJOINQTY,
         |	FRETURNJOINQTY,
         |	FBASERETURNJOINQTY,
         |	FBFLOWID,
         |	FMTONO,
         |	FPROJECTNO,
         |	FGIVEAWAY,
         |	FSAMPLEDAMAGEQTY,
         |	FSAMPLEDAMAGEBASEQTY,
         |	FCHECKINCOMING,
         |	FEXTAUXUNITID,
         |	FEXTAUXUNITQTY,
         |	FRECEIVEMTONO,
         |	FISRECEIVEUPDATESTOCK,
         |	FWWINTYPE,
         |	FPOORDERENTRYID,
         |	FSRCENTRYID,
         |	F_PAEZ_TEXT,
         |	F_PAEZ_BASE,
         |	F_PAEZ_BASE1,
         |	F_PAEZ_BASE2,
         |	F_PAEZ_BASE3,
         |	FZWHXNO,
         |	F_PXDF_TEXT,
         |	F_PXDF_AMOUNT,
         |	F_PROJECTNO,
         |	F_PXDF_AMOUNT1,
         |	F_PXDF_AMOUNT2,
         |	F_PXDF_PRICE,
         |	FCMKBARCODE,
         |	FALLOTBASEQTY,
         |	FDELIVERYNAME,
         |	FISSCANENTRY,
         |	F_PAEZ_TEXT2,
         |	F_PAEZ_PRICE,
         |	FALLAMOUNTEXCEPTDISCOUNT,
         |	FSALOUTSTOCKBILLNO,
         |	FSALOUTSTOCKENTRYID,
         |	FISRECONCILIATIONING,
         |	FRECONCILIATIONBILLNO,
         |	FALLRECONCILIATIONBILLNO,
         |	F_YT,
         |	F_TIMESTAMP
         |FROM
         |  INSTOCKENTRY
         |where
         |rank = 1
         |""".stripMargin)


    //ODS_ERP_INSTOCKENTRY_F表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FENTRYID order by F_TIMESTAMP desc) rank
         |FROM
         |${TableName.ODS_ERP_INSTOCKENTRY_F}
         |""".stripMargin).createOrReplaceTempView("INSTOCKENTRY_F")
    spark.sql(
      s"""
         |insert overwrite table ${TableName.ODS_ERP_INSTOCKENTRY_F}
         |select
         |  FENTRYID,
         |	FID,
         |	FPRICE,
         |	FTAXPRICE,
         |	FPRICEUNITID,
         |	FPRICEUNITQTY,
         |	FTAXRATE,
         |	FTAXAMOUNT,
         |	FTAXAMOUNT_LC,
         |	FPRICECOEFFICIENT,
         |	FDISCOUNTRATE,
         |	FDISCOUNT,
         |	FBILLCOSTAPPORTION,
         |	FTAXNETPRICE,
         |	FAMOUNT,
         |	FAMOUNT_LC,
         |	FALLAMOUNT,
         |	FALLAMOUNT_LC,
         |	FISFREE,
         |	FBASEUNITPRICE,
         |	FINVOICEDQTY,
         |	FPROCESSFEE,
         |	FMATERIALCOSTS,
         |	FJOINEDQTY,
         |	FUNJOINQTY,
         |	FJOINEDAMOUNT,
         |	FUNJOINAMOUNT,
         |	FFULLYJOINED,
         |	FJOINSTATUS,
         |	FMAXPRICE,
         |	FMINPRICE,
         |	FTAXCOMBINATION,
         |	FCOSTPRICE,
         |	FCOSTAMOUNT,
         |	FCOSTAMOUNT_LC,
         |	FSYSPRICE,
         |	FUPPRICE,
         |	FDOWNPRICE,
         |	FINVOICEDSTATUS,
         |	FINVOICEDJOINQTY,
         |	FBILLINGCLOSE,
         |	FCOSTPRICE_LC,
         |	FPRICEBASEQTY,
         |	FSETPRICEUNITID,
         |	FREMAININSTOCKQTY,
         |	FREMAININSTOCKBASEQTY,
         |	FREMAININSTOCKUNITID,
         |	FAPNOTJOINQTY,
         |	FAPJOINAMOUNT,
         |	FPRICELISTENTRY,
         |	FPROCESSFEE_LC,
         |	FMATERIALCOSTS_LC,
         |	F_PAEZ_AMOUNT,
         |	FREJECTSDISCOUNTAMOUNT,
         |	F_TIMESTAMP
         |FROM
         |  INSTOCKENTRY_F
         |where
         |rank = 1
         |""".stripMargin)


    //ODS_ERP_DELIVERYNOTICE表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FID order by FMODIFYDATE desc) rank
         |FROM
         |${TableName.ODS_ERP_DELIVERYNOTICE}
         |""".stripMargin).createOrReplaceTempView("DELIVERYNOTICE")
    spark.sql(
      s"""
         |insert overwrite table ${TableName.ODS_ERP_DELIVERYNOTICE}
         |select
         |  FID,
         |	FBILLTYPEID,
         |	FBILLNO,
         |	FDATE,
         |	FCUSTOMERID,
         |	FDELIVERYORGID,
         |	FDELIVERYDEPTID,
         |	FSTOCKERGROUPID,
         |	FSTOCKERID,
         |	FRECEIVERID,
         |	FSETTLEID,
         |	FPAYERID,
         |	FDELIVERYNO,
         |	FTAKEDELIVERYNO,
         |	FCARRIAGENO,
         |	FCARRIERID,
         |	FSALEORGID,
         |	FSALEGROUPID,
         |	FSALEDEPTID,
         |	FSALESMANID,
         |	FNOTE,
         |	FDOCUMENTSTATUS,
         |	FCANCELSTATUS,
         |	FCREATORID,
         |	FCREATEDATE,
         |	FMODIFIERID,
         |	FMODIFYDATE,
         |	FAPPROVERID,
         |	FAPPROVEDATE,
         |	FCANCELLERID,
         |	FCANCELDATE,
         |	FOWNERTYPEID,
         |	FOWNERID,
         |	FCLOSERID,
         |	FCLOSEDATE,
         |	FCLOSESTATUS,
         |	FRECEIPTCONDITIONID,
         |	FHEADLOCID,
         |	FHEADLOCADDRESS,
         |	FHEADDELIVERYWAY,
         |	FISSYSCLOSE,
         |	FBUSINESSTYPE,
         |	FRECEIVEADDRESS,
         |	FCREDITCHECKRESULT,
         |	FOBJECTTYPEID,
         |	FCORRESPONDORGID,
         |	FRECCONTACTID,
         |	F_PAEZ_TEXT,
         |	F_PAEZ_TEXT1,
         |	F_PAEZ_TEXT2,
         |	F_PAEZ_TEXT3,
         |	F_PAEZ_TEXT4,
         |	FPLANRECADDRESS,
         |	F_PAEZ_BASE1,
         |	F_PAEZ_CHECKBOX,
         |	F_PAEZ_BASE2,
         |	F_PAEZ_REMARKS,
         |	F_PAEZ_BASE3,
         |	F_PAEZ_CHECKBOX1,
         |	FISMANUALCLOSE,
         |	F_PAEZ_TEXT6,
         |	F_PAEZ_TEXT7,
         |	F_PAEZ_TEXT8,
         |	FOMSDELIVERYID,
         |	F_PXDF_TEXT,
         |	FOMSDELIVERY,
         |	FLOGISTICSCOMPANY,
         |	F_PXDF_TEXT1,
         |	F_PXDF_TEXT2,
         |	F_PXDF_TEXT3,
         |	FPROJECTNO,
         |	FPROJECTNAME,
         |	FSRCBILL,
         |	FISSYNCHRO,
         |	FISFROMOMS,
         |	FISSYNCHROTOWMS,
         |	FISAUTOPUSH,
         |	FLOGISTICSNO,
         |	FLOGISTICS,
         |	FOMSMAINORDERID,
         |	F_PXDF_CHECKBOX,
         |	FORDERTYPE,
         |	FPURTYPE,
         |	F_PXDF_COMBO1,
         |	F_PXDF_TEXT8,
         |	F_PXDF_PRINTTIMES,
         |	F_PXDF_PRINTDATETIME,
         |	FMERCHANDISER,
         |	F_PXDF_TEXT10,
         |	F_PXDF_TEXT11,
         |	F_PXDF_TEXT12,
         |	F_PXDF_COMBO,
         |	F_PAEZ_COMBO,
         |	F_PXDF_TEXT121,
         |	FXMGLPROJECTNO,
         |	F_PAEZ_TEXT9,
         |	F_PAEZ_TEXT10,
         |	F_PAEZ_ATTACHMENTCOUNT,
         |	F_PAEZ_BASE31,
         |	F_PAEZ_TEXT15,
         |	F_PAEZ_TEXT16,
         |	F_PAEZ_BASE5,
         |	F_PAEZ_BASE6,
         |	F_PAEZ_TEXT17,
         |	F_PAEZ_TEXT151,
         |	F_PAEZ_BASE7,
         |	F_PAEZ_BASE8,
         |	F_PAEZ_CHECKBOX3,
         |	F_PAEZ_CHECKBOX4,
         |	FLINKMAN,
         |	FLINKPHONE,
         |	FCLOSEREASON,
         |	F_PAEZ__PAEZ_CHECKBOX,
         |	F_PAEZ__PXDF_TEXT9,
         |	FLOGISTICSNO1,
         |	FISSOURCESRM,
         |	FSTKINBILLNO
         |FROM
         |  DELIVERYNOTICE
         |where
         |rank = 1
         |""".stripMargin)


    //ODS_ERP_DELIVERYNOTICEENTRY表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FENTRYID order by F_TIMESTAMP desc) rank
         |FROM
         |${TableName.ODS_ERP_DELIVERYNOTICEENTRY}
         |""".stripMargin).createOrReplaceTempView("DELIVERYNOTICEENTRY")
    spark.sql(
      s"""
         |insert overwrite table ${TableName.ODS_ERP_DELIVERYNOTICEENTRY}
         |select
         |  FENTRYID,
         |	FID,
         |	FSEQ,
         |	FMATERIALID,
         |	FAUXPROPID,
         |	FUNITID,
         |	FQTY,
         |	FSHIPMENTSTOCKID,
         |	FSHIPMENTSTOCKLOCID,
         |	FDELIVERYLOC,
         |	FDELIVERYADDRESS,
         |	FDELIVERYDATE,
         |	FDELIVERYTYPE,
         |	FPLANDELIVERYDATE,
         |	FCARRYLEADTIME,
         |	FBOMID,
         |	FLOT,
         |	FLOT_TEXT,
         |	FBASEUNITID,
         |	FBASEUNITQTY,
         |	FGROSSWEIGHT,
         |	FNETWEIGHT,
         |	FOUTCONTROL,
         |	FOUTMAXQTY,
         |	FOUTMINQTY,
         |	FSRCTYPE,
         |	FSRCBILLNO,
         |	FSRCROWID,
         |	FNOTE,
         |	FJOINOUTQTY,
         |	FBASEJOINOUTQTY,
         |	FSUMOUTQTY,
         |	FBASESUMRETNOTICEQTY,
         |	FORDERNO,
         |	FORDERSEQ,
         |	FBACKUPSTOCKID,
         |	FBACKUPSTOCKLOCID,
         |	FTRANSFERQTY,
         |	FBASETRANSFERQTY,
         |	FBASEACTUALQTY,
         |	FBASESUMOUTQTY,
         |	FPRODUCEDATE,
         |	FEXPUNIT,
         |	FEXPPERIOD,
         |	FEXPIRYDATE,
         |	FREMAINOUTQTY,
         |	FCLOSESTATUS,
         |	FSTOCKSTATUSID,
         |	FCUSTMATID,
         |	FBASEOUTMAXQTY,
         |	FBASEOUTMINQTY,
         |	FBFLOWID,
         |	FMTONO,
         |	F_PAEZ_BASE,
         |	F_PAEZ_INTEGER,
         |	F_PAEZ_BASE2,
         |	F_PAEZ_TEXT5,
         |	F_PAEZ_BASEQTY,
         |	F_PAEZ_BASE4,
         |	F_PAEZ_CHECKBOX2,
         |	F_PXDF_TEXT,
         |	F_PXDF_TEXT1,
         |	FPROJECTNO,
         |	FPROJECTNAME,
         |	FROW,
         |	F_PXDF_TEXT2,
         |	F_PXDF_TEXT3,
         |	FOLDPROJECTNO,
         |	FPRONO,
         |	F_PXDF_TEXT4,
         |	F_PXDF_PRICE,
         |	F_PAEZ_REMARKS1,
         |	F_PAEZ_DECIMAL,
         |	F_PAEZ_AMOUNT,
         |	F_PAEZ_TEXT11,
         |	F_PAEZ_TEXT12,
         |	F_PAEZ_TEXT13,
         |	F_PAEZ_TEXT14,
         |	F_PAEZ_PRICE,
         |	F_PAEZ_AMOUNT1,
         |	FALLAMOUNTEXCEPTDISCOUNT,
         |	FLOCKSTOCKFLAG,
         |	FLOCKSTOCKBASEQTY,
         |	FLOCKSTOCKLEFTBASEQTY,
         |	FSNUNITID,
         |	FSNQTY,
         |	F_UN_JOINPREPICKBASEQTY,
         |	F_PAEZ_TEXT16,
         |	F_TIMESTAMP
         |FROM
         |  DELIVERYNOTICEENTRY
         |where
         |rank = 1
         |""".stripMargin)


    //ODS_ERP_OUTSTOCK表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FID order by FMODIFYDATE desc) rank
         |FROM
         |${TableName.ODS_ERP_OUTSTOCK}
         |""".stripMargin).createOrReplaceTempView("OUTSTOCK")
    spark.sql(
      s"""
         |insert overwrite table ${TableName.ODS_ERP_OUTSTOCK}
         |select
         |  FID,
         |	FBILLTYPEID,
         |	FBILLNO,
         |	FDATE,
         |	FCUSTOMERID,
         |	FSTOCKORGID,
         |	FDELIVERYDEPTID,
         |	FSTOCKERGROUPID,
         |	FSTOCKERID,
         |	FRECEIVERID,
         |	FSETTLEID,
         |	FPAYERID,
         |	FSALEORGID,
         |	FSALEDEPTID,
         |	FSALESGROUPID,
         |	FSALESMANID,
         |	FDELIVERYBILL,
         |	FTAKEDELIVERYBILL,
         |	FCARRIERID,
         |	FCARRIAGENO,
         |	FDOCUMENTSTATUS,
         |	FNOTE,
         |	FCREATORID,
         |	FCREATEDATE,
         |	FMODIFIERID,
         |	FMODIFYDATE,
         |	FAPPROVERID,
         |	FAPPROVEDATE,
         |	FCANCELSTATUS,
         |	FCANCELLERID,
         |	FCANCELDATE,
         |	FOWNERTYPEID,
         |	FOWNERID,
         |	FHEADLOCID,
         |	FHEADLOCADDRESS,
         |	FHEADLOCATIONID,
         |	FBUSINESSTYPE,
         |	FRECEIVEADDRESS,
         |	FCREDITCHECKRESULT,
         |	FOBJECTTYPEID,
         |	FTRANSFERBIZTYPE,
         |	FCORRESPONDORGID,
         |	FRECCONTACTID,
         |	FISINTERLEGALPERSON,
         |	FPLANRECADDRESS,
         |	FISTOTALSERVICEORCOST,
         |	FSRCFID,
         |	FDISASSEMBLYFLAG,
         |	F_PAEZ_COMBO,
         |	F_PAEZ_TEXT,
         |	F_PAEZ_TEXT1,
         |	F_PAEZ_TEXT2,
         |	F_PAEZ_TEXT3,
         |	F_PAEZ_TEXT4,
         |	F_PAEZ_TEXT5,
         |	F_PAEZ_TEXT7,
         |	F_PAEZ_TEXT8,
         |	F_PAEZ_TEXT9,
         |	F_PAEZ_BASE,
         |	F_PAEZ_DECIMAL,
         |	F_PAEZ_CHECKBOX,
         |	F_PAEZ_TEXT6,
         |	F_PAEZ_TEXT10,
         |	F_PAEZ_PRINTDATETIME,
         |	F_PAEZ_REMARKS,
         |	F_PAEZ_TEXT11,
         |	F_PAEZ_LARGETEXT,
         |	F_PAEZ_REMARKS1,
         |	F_PAEZ_TEXT13,
         |	F_PAEZ_DATE,
         |	F_PAEZ_CHECKBOX1,
         |	F_PAEZ_TEXT15,
         |	F_PAEZ_TEXT16,
         |	F_PAEZ_TEXT17,
         |	FOMSDELIVERYID,
         |	F_PXDF_TEXT,
         |	FOMSDELIVERY,
         |	FLOGISTICSNO,
         |	FLOGISTICSCOMPANY,
         |	F_PXDF_TEXT1,
         |	FISAUTOPUSH,
         |	FISSYNCHROTOWMS,
         |	FLOGISTICS,
         |	F_ORDERTYPE,
         |	F_PXDF_TEXT5,
         |	F_PXDF_TEXT6,
         |	F_PXDF_TEXT61,
         |	F_PXDF_TEXT62,
         |	F_PXDF_TEXT51,
         |	F_PXDF_REMARKS,
         |	F_PXDF_TEXT811,
         |	FSYB,
         |	FFPLX,
         |	FMERCHANDISER,
         |	F_PXDF_COMBO,
         |	F_PXDF_PRINTTIMES,
         |	F_PXDF_TEXT10,
         |	F_PXDF_TEXT11,
         |	F_PXDF_TEXT12,
         |	F_PXDF_CHECKBOX,
         |	F_PXDF_COMBO1,
         |	FSHOPNUMBER,
         |	FGENFROMPOS_CMK,
         |	FLINKMAN,
         |	FLINKPHONE,
         |	FBRANCHID,
         |	FISUNAUDIT,
         |	F_PAEZ_COMBO1,
         |	F_PXDF_TEXT121,
         |	FXMGLPROJECTNO,
         |	F_PAEZ_BASE4,
         |	F_PAEZ_TEXT19,
         |	F_PAEZ_TEXT20,
         |	FISSENDOMS,
         |	FGYDATE,
         |	FSALECHANNEL,
         |	FLOGISTICSNOS,
         |	F_PAEZ_BASE31,
         |	F_PAEZ_TEXT25,
         |	F_PAEZ_TEXT251,
         |	F_PAEZ_BASE5,
         |	F_PAEZ_BASE51,
         |	F_PAEZ_CHECKBOX3,
         |	F_PAEZ_CHECKBOX4,
         |	F_PAEZ_AMOUNT3,
         |	FISDEALSTOCK,
         |	F_PAEZ__PAEZ_CHECKBOX,
         |	F_PAEZ__PXDF_TEXT9,
         |	FISSOURCESRM,
         |	FSTKINBILLNO,
         |	F_PAEZ_TBOMS
         |FROM
         |  OUTSTOCK
         |where
         |rank = 1
         |""".stripMargin)


    //ODS_ERP_OUTSTOCKENTRY表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FENTRYID order by F_TIMESTAMP desc) rank
         |FROM
         |${TableName.ODS_ERP_OUTSTOCKENTRY}
         |""".stripMargin).createOrReplaceTempView("OUTSTOCKENTRY")
    spark.sql(
      s"""
         |insert overwrite table ${TableName.ODS_ERP_OUTSTOCKENTRY}
         |select
         |  FENTRYID,
         |	FID,
         |	FCUSTMATNAME,
         |	FSEQ,
         |	FMATERIALID,
         |	FUNITID,
         |	FAUXPROPID,
         |	FMUSTQTY,
         |	FREALQTY,
         |	FSTOCKID,
         |	FSTOCKLOCID,
         |	FSTOCKSTATUSID,
         |	FLOT,
         |	FLOT_TEXT,
         |	FGROSSWEIGHT,
         |	FNETWEIGHT,
         |	FBASEUNITID,
         |	FBASEUNITQTY,
         |	FAUXUNITID,
         |	FAUXUNITQTY,
         |	FBOMID,
         |	FNOTE,
         |	FSTOCKFLAG,
         |	FOWNERTYPEID,
         |	FOWNERID,
         |	FKEEPERTYPEID,
         |	FKEEPERID,
         |	FPRODUCEDATE,
         |	FEXPIRYDATE,
         |	FBASEMUSTQTY,
         |	FARRIVALSTATUS,
         |	FARRIVALDATE,
         |	FARRIVALCONFIRMOR,
         |	FVALIDATESTATUS,
         |	FVALIDATEDATE,
         |	FVALIDATECONFIRMOR,
         |	FCUSTMATID,
         |	FBFLOWID,
         |	FMTONO,
         |	FPROJECTNO,
         |	FREPAIRQTY,
         |	FREFUSEQTY,
         |	FWANTRETQTY,
         |	FACTQTY,
         |	FISREPAIR,
         |	FRECNOTE,
         |	FRETURNNOTE,
         |	FSNUNITID,
         |	FSNQTY,
         |	FOUTCONTROL,
         |	FEXTAUXUNITID,
         |	FEXTAUXUNITQTY,
         |	FSRCENTRYID,
         |	F_PAEZ_TEXT6,
         |	F_PAEZ_BASE1,
         |	F_PAEZ_TEXT12,
         |	F_PAEZ_TEXT13,
         |	F_PAEZ_BASE2,
         |	F_PAEZ_AMOUNT,
         |	F_PAEZ_CHECKBOX1,
         |	F_PAEZ_BASE3,
         |	F_PAEZ_TEXT14,
         |	FREFERPRICE,
         |	FREFERAMOUNT,
         |	FPROFIT,
         |	F_PXDF_TEXT,
         |	F_PXDF_TEXT1,
         |	F_PXDF_TEXT2,
         |	F_PXDF_TEXT3,
         |	F_PXDF_TEXT4,
         |	F_OLDPROJECTNO,
         |	F_PROJECTNO,
         |	F_PXDF_DECIMAL,
         |	F_PXDF_PRICE,
         |	FBARCODE,
         |	FRETAILSALEPROM,
         |	F_PAEZ_TEXT18,
         |	F_PAEZ_AMOUNT1,
         |	F_PAEZ_PRICE,
         |	F_PAEZ_CHECKBOX2,
         |	F_PAEZ_COMBO2,
         |	F_PAEZ_COMBO3,
         |	F_PAEZ_BASE4,
         |	F_PAEZ_TEXT21,
         |	F_PAEZ_TEXT22,
         |	F_PAEZ_TEXT23,
         |	F_PAEZ_TEXT24,
         |	F_PAEZ_PRICE1,
         |	F_PAEZ_AMOUNT2,
         |	FALLAMOUNTEXCEPTDISCOUNT,
         |	FGYENTERTIME,
         |	FMATERIALID_SAL,
         |	FINSTOCKBILLNO,
         |	FINSTOCKENTRYID,
         |	F_PAEZ_TEXT26,
         |	F_TIMESTAMP
         |FROM
         |  OUTSTOCKENTRY
         |where
         |rank = 1
         |""".stripMargin)

    //ODS_ERP_OUTSTOCKENTRY_F表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FENTRYID order by F_TIMESTAMP desc) rank
         |FROM
         |${TableName.ODS_ERP_OUTSTOCKENTRY_F}
         |""".stripMargin).createOrReplaceTempView("OUTSTOCKENTRY_F")
    spark.sql(
      s"""
         |insert overwrite table ${TableName.ODS_ERP_OUTSTOCKENTRY_F}
         |select
         |  FENTRYID,
         |	FID,
         |	FPRICE,
         |	FTAXPRICE,
         |	FPRICECOEFFICIENT,
         |	FSYSPRICE,
         |	FLIMITDOWNPRICE,
         |	FUPPRICE,
         |	FDOWNPRICE,
         |	FPRICEUNITID,
         |	FPRICEUNITQTY,
         |	FTAXRATE,
         |	FTAXAMOUNT,
         |	FTAXAMOUNT_LC,
         |	FTAXNETPRICE,
         |	FDISCOUNTRATE,
         |	FDISCOUNT,
         |	FBILLDISAPPORTION,
         |	FBILLCOSTAPPORTION,
         |	FBEFBILLDISAMT,
         |	FBEFBILLDISALLAMT,
         |	FBEFDISAMT,
         |	FBEFDISALLAMT,
         |	FAMOUNT,
         |	FAMOUNT_LC,
         |	FALLAMOUNT,
         |	FALLAMOUNT_LC,
         |	FTAXCOMBINATION,
         |	FSALCOSTPRICE,
         |	FCOSTPRICE,
         |	FCOSTAMOUNT,
         |	FCOSTAMOUNT_LC,
         |	FISFREE,
         |	FISCONSUMESUM,
         |	FISOVERLEGALORG,
         |	FSALUNITID,
         |	FSALBASEQTY,
         |	FSALUNITQTY,
         |	FPRICEBASEQTY,
         |	FSALBASENUM,
         |	FSTOCKBASEDEN,
         |	FSRCBIZUNITID,
         |	FISCREATEPRODOC,
         |	FPRICELISTENTRY,
         |	FQUALIFYTYPE,
         |	FROWTYPE,
         |	FROWID,
         |	FPARENTROWID,
         |	FPARENTMATID,
         |	FPRICEDISCOUNT,
         |	FPROPRICE,
         |	FPROAMOUNT,
         |	FTAILDIFFFLAG,
         |	FSETTLEBYSON,
         |	F_TIMESTAMP
         |FROM
         |  OUTSTOCKENTRY_F
         |where
         |rank = 1
         |""".stripMargin)

    //ODS_ERP_OUTSTOCKENTRY_R表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FENTRYID order by F_TIMESTAMP desc) rank
         |FROM
         |${TableName.ODS_ERP_OUTSTOCKENTRY_R}
         |""".stripMargin).createOrReplaceTempView("OUTSTOCKENTRY_R")
    spark.sql(
      s"""
         |insert overwrite table ${TableName.ODS_ERP_OUTSTOCKENTRY_R}
         |select
         |  FENTRYID,
         |	FID,
         |	FSRCTYPE,
         |	FSRCBILLNO,
         |	FJOINEDQTY,
         |	FUNJOINQTY,
         |	FJOINEDAMOUNT,
         |	FUNJOINAMOUNT,
         |	FFULLYJOINED,
         |	FJOINSTATUS,
         |	FRETURNQTY,
         |	FBASERETURNQTY,
         |	FSUMRETNOTICEQTY,
         |	FSUMRETSTOCKQTY,
         |	FINVOICEDQTY,
         |	FBASEINVOICEDQTY,
         |	FSUMINVOICEDQTY,
         |	FSUMINVOICEDAMT,
         |	FSUMRECIEVEDAMT,
         |	FSOORDERNO,
         |	FBASESUMRETNOTICEQTY,
         |	FBASESUMRETSTOCKQTY,
         |	FBASESUMINVOICEDQTY,
         |	FBASEARJOINQTY,
         |	FBASEARQTY,
         |	FARJOINAMOUNT,
         |	FARAMOUNT,
         |	FBASEJOININSTOCKQTY,
         |	FJOININSTOCKQTY,
         |	FSECJOININSTOCKQTY,
         |	FSECRETURNQTY,
         |	FARJOINQTY,
         |	FEOWNERSUPPLIERID,
         |	FESETTLECUSTOMERID,
         |	FSTOCKBASERETURNQTY,
         |	FSTOCKBASESUMRETSTOCKQTY,
         |	FSTOCKBASEARJOINQTY,
         |	FSALBASEARJOINQTY,
         |	FPURBASEJOININSTOCKQTY,
         |	FARNOTJOINQTY,
         |	FQMENTRYID,
         |	FCONVERTENTRYID,
         |	FB2CORDERDETAILID,
         |	FSOENTRYID,
         |	FRESERVEENTRYID,
         |	FSIGNQTY,
         |	FCHECKDELIVERY,
         |	FGYFINSTATUS,
         |	FTHIRDENTRYID,
         |	FTHIRDBILLNO,
         |	FTHIRDBILLID,
         |	FGYFINDATE,
         |	FWRITEOFFPRICEBASEQTY,
         |	FWRITEOFFSALEBASEQTY,
         |	FWRITEOFFSTOCKBASEQTY,
         |	FWRITEOFFAMOUNT,
         |	FBOMENTRYID,
         |	F_TIMESTAMP
         |FROM
         |  OUTSTOCKENTRY_R
         |where
         |rank = 1
         |""".stripMargin)

    //DIM_LOTMASTER表增量更新
    spark.sql(
      s"""
         |SELECT
         | *,
         | row_number() over(partition by FLOTID order by FMODIFYDATE desc) rank
         |FROM
         |${TableName.DIM_LOTMASTER}
         |""".stripMargin).createOrReplaceTempView("LOTMASTER")
    spark.sql(
      s"""
         |insert overwrite table ${TableName.DIM_LOTMASTER}
         |select
         |  FLOTID,
         |	FMASTERID,
         |	FMATERIALID,
         |	FAUXPROPERTYID,
         |	FNUMBER,
         |	FLOTSTATUS,
         |	FDOCUMENTSTATUS,
         |	FSUPPLYID,
         |	FSUPPLYLOT,
         |	FPRODUCEDEPTID,
         |	FCREATEORGID,
         |	FUSEORGID,
         |	FCREATORID,
         |	FCREATEDATE,
         |	FMODIFIERID,
         |	FMODIFYDATE,
         |	FPRODUCEDATE,
         |	FEXPIRYDATE,
         |	FFORBIDSTATUS,
         |	FBIZTYPE,
         |	FCUSTID,
         |	FCANCELSTATUS,
         |	FINSTOCKDATE,
         |	FPKID,
         |	FLOCALEID,
         |	FNAME,
         |	FDESCRIPTION
         |FROM
         |  LOTMASTER
         |where
         |rank = 1
         |""".stripMargin)
  }
}

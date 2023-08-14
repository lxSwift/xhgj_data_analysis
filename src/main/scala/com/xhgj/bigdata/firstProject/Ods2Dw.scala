package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/6/14 11:05
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: Ods2Dw
 * @Description: TODO
 * @Version 1.0
 */
object Ods2Dw {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job Ods2Dw.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession): Unit = {
    import spark.implicits._
//    采购申请单表DWD_PUR_REQUISITION
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWD_PUR_REQUISITION}
         |SELECT
         |OER.FID
         |,OERE.FENTRYID
         |,OER.FBILLNO
         |,OER.FDOCUMENTSTATUS
         |,OER.FCREATORID
         |,OER.FCREATEDATE
         |,OER.FAPPROVERID
         |,OER.FAPPROVEDATE
         |,OERE.F_PROJECTNO
         |,OER.F_PXDF_ORGID
         |,OERE.F_PAEZ_BASE1
         |,OERR.FSRCBILLNO
         |,OERE.FREQQTY
         |,OERE.FAPPROVEQTY
         |,OERE.FMATERIALID
         |,OER.FAPPLICATIONORGID
         |,OER.FAPPLICATIONDATE
         |FROM ${TableName.ODS_ERP_REQUISITION} oer
         |LEFT JOIN ${TableName.ODS_ERP_REQENTRY} oere ON oere.FID = oer.FID
         |LEFT JOIN ${TableName.ODS_ERP_REQENTRY_R} oerr ON oere.fentryid = oerr.fentryid
         |""".stripMargin)

    //--采购订单表DWD_PUR_POORDER
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWD_PUR_POORDER}
         |SELECT
         |OEP.FID
         |,OEPE.FENTRYID
         |,OEP.FBILLNO
         |,OEP.FDOCUMENTSTATUS
         |,OEP.FSUPPLIERID
         |,OEP.FPURCHASEDEPTID
         |,OEP.FPURCHASERID
         |,OEP.FCREATEDATE
         |,OEP.FCREATORID
         |,OEP.FAPPROVEDATE
         |,OEP.FAPPROVERID
         |,OEPE.FMATERIALID
         |,OEPE.FQTY
         |,OEPF.FPRICE
         |,OEPF.FAMOUNT
         |,OEPF.FTAXPRICE
         |,OEPF.FALLAMOUNT
         |,OEPR.FSRCBILLNO
         |,OEP.FPURCHASEORGID
         |,OEP.FDATE
         |FROM ${TableName.ODS_ERP_POORDER} oep
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY} oepe on oep.fid = oepe.fid
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY_F} oepf on oepe.fentryid = oepf.fentryid
         |LEFT JOIN ${TableName.ODS_ERP_POORDERENTRY_R} oepr on oepe.Fentryid = oepr.fentryid
         |""".stripMargin)

    //--采购入库单表DWD_PUR_INSTOCK
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWD_PUR_INSTOCK}
         |SELECT
         |OEI.FID,
         |OEIE.FENTRYID ,
         |OEI.FBILLNO,
         |OEI.FDOCUMENTSTATUS,
         |OEI.FCREATORID,
         |OEI.FCREATEDATE,
         |OEI.FAPPROVERID,
         |OEI.FAPPROVEDATE,
         |OEIE.FMATERIALID,
         |OEIE.FMUSTQTY,
         |OEIE.FREALQTY,
         |OEIF.FAPNOTJOINQTY,
         |OEIE.FGIVEAWAY,
         |OEIE.FSTOCKID,
         |OEIE.FSRCBILLNO,
         |DL.FNAME FLOTNAME,
         |OEI.FSTOCKORGID,
         |OEI.FDATE,
         |OEIF.F_PAEZ_AMOUNT
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_F} oeif ON oeif.FENTRYID = oeie.FENTRYID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oeie.FLOT = dl.FLOTID
         |""".stripMargin)

    //--发货通知单表DWD_SAL_DELIVERYNOTICE
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWD_SAL_DELIVERYNOTICE}
         |SELECT
         | OED.FID
         |,OEDE.FENTRYID
         |,OED.FBILLNO
         |,OED.FDOCUMENTSTATUS
         |,OED.FCREATEDATE
         |,OED.FCREATORID
         |,OED.FAPPROVEDATE
         |,OED.FAPPROVERID
         |,OEDE.FMATERIALID
         |,OEDE.FQTY
         |,OEDE.FSRCBILLNO
         |,DL.FNAME FLOTNAME
         |,OED.FDELIVERYORGID
         |,OED.FDATE
         |FROM ${TableName.ODS_ERP_DELIVERYNOTICE} oed
         |LEFT JOIN ${TableName.ODS_ERP_DELIVERYNOTICEENTRY} oede ON oede.FID = oed.FID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oede.FLOT = dl.FLOTID
         |""".stripMargin)

    //--销售出库单表DWD_SAL_OUTSTOCK
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWD_SAL_OUTSTOCK}
         |SELECT
         |OEO.FID
         |,OEOE.FENTRYID
         |,OEO.FBILLNO
         |,OEO.FCREATEDATE
         |,OEO.FCREATORID
         |,OEO.FAPPROVEDATE
         |,OEO.FAPPROVERID
         |,OEO.FDOCUMENTSTATUS
         |,OEOE.FMATERIALID
         |,OEOE.FMUSTQTY
         |,OEOE.FREALQTY
         |,OEOF.FPRICE
         |,OEOF.FAMOUNT
         |,OEOF.FTAXPRICE
         |,OEOF.FALLAMOUNT
         |,OEOR.FSRCBILLNO
         |,OEO.FSTOCKORGID
         |,OEO.FDATE
         |,OEOR.FARJOINQTY
         |,OEOR.FARJOINAMOUNT
         |,OEOR.FARAMOUNT
         |,OEOR.FARNOTJOINQTY
         |,OEOE.F_PAEZ_AMOUNT
         |FROM ${TableName.ODS_ERP_OUTSTOCK} oeo
         |LEFT JOIN ${TableName.ODS_ERP_OUTSTOCKENTRY} oeoe ON oeo.FID = oeoe.FID
         |LEFT JOIN ${TableName.ODS_ERP_OUTSTOCKENTRY_F} oeof  ON oeoe.FENTRYID = oeof.FENTRYID
         |LEFT JOIN ${TableName.ODS_ERP_OUTSTOCKENTRY_R} oeor  ON oeoe.FENTRYID = oeor.FENTRYID
         |""".stripMargin)

    //--应收结算清单-物料表DWD_FIN_ARSETTLEMENT
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWD_FIN_ARSETTLEMENT}
         |SELECT
         |OEA.FID
         |,OEAE.FDETAILID
         |,OEA.FBILLNO
         |,OEA.FCREATORID
         |,OEA.FCREATEDATE
         |,OEA.FAPPROVEID
         |,OEA.FAPPROVEDATE
         |,OEA.FDOCUMENTSTATUS
         |,OEAE.FMATERIALID
         |,OEAE.FQTY
         |,OEAE.FPRICE
         |,OEAE.FTAXPRICE
         |,OEAE.FAMOUNT
         |,OEAE.FALLAMOUNT
         |,OEA.FACCTORGID
         |,OEA.FDATE
         |,OEAE.FREFERAMOUNT
         |,OEAE.FREFERQTY
         |,OEAE.F_PXDF_QTY
         |,OEAE.F_PXDF_AMOUNT
         |,OEAE.F_PXDF_TEXT4
         | FROM ${TableName.ODS_ERP_ARSETTLEMENT} oea
         | LEFT JOIN ${TableName.ODS_ERP_ARSETTLEMENTDETAIL} oeae ON oea.FID = oeae.FID
         |""".stripMargin)

    //销售订单DWD_SAL_ORDER
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWD_SAL_ORDER}
         |SELECT
         |OES.FID
         |,OESE.FENTRYID
         |,OES.FCREATEDATE
         |,OES.FCREATORID
         |,OES.FAPPROVEDATE
         |,OES.FAPPROVERID
         |,OES.F_PAEZ_TEXT13
         |,OES.F_PAEZ_TEXT14
         |,OES.F_PAEZ_TEXT
         |,OES.F_PAEZ_TEXT1
         |,OES.F_PAEZ_TEXT2
         |,OESE.FQTY
         |,OESF.FTAXPRICE
         |,OESF.FPRICE
         |,OESF.FALLAMOUNT
         |,OESF.FAMOUNT
         |,OES.FPURTYPE
         |,OESE.FMRPTERMINATESTATUS
         |,OES.F_PAEZ_CHECKBOX
         |,OES.FBILLNO
         |,OESE.FNOTE
         |,OES.FDOCUMENTSTATUS
         |,OES.FPROJECTBASIC
         |,OES.FSALERID
         |,OES.F_PAEZ_BASE3
         |,OESE.FUNITID
         |,OESE.FMATERIALID
         |,OES.FCUSTID
         |,OES.FSALEORGID
         |,OES.FDATE
         |,OES.F_ORDERTYPE
         |,OESE.F_PROJECTNO
         |FROM ${TableName.ODS_ERP_SALORDER} OES
         |LEFT JOIN ${TableName.ODS_ERP_SALORDERENTRY} OESE ON OES.FID = OESE.FID
         |LEFT JOIN ${TableName.ODS_ERP_SALORDERENTRY_F} OESF ON OESE.FENTRYID = OESF.FENTRYID
         |""".stripMargin)


    //OA的人员信息表
    //先筛选出组织级别为1或者2 且可用的所有组织
    spark.sql(
      s"""
         |SELECT
         |  *
         |FROM
         |  ${TableName.ODS_OA_ORG_ELEMENT} A
         |WHERE
         |  fd_org_type in ('1','2') and fd_is_available !=0
         |""".stripMargin).createOrReplaceTempView("ORGELE")


    val res = spark.sql(
      s"""
         |SELECT
         |  ORG.fd_name deptname,
         |  ORG_L.fd_name parentname,
         |  B.fd_name name,
         |  B.fd_name_pinyin name_pinyin,
         |  A.fd_mobile_no mobile,
         |  A.fd_email email,
         |  ORG.fd_id deptid,
         |  ORG_L.fd_id parentid
         |FROM ORGELE ORG
         |LEFT JOIN ORGELE ORG_L ON ORG.fd_parentid=ORG_L.fd_id
         |LEFT JOIN ${TableName.ODS_OA_ORG_ELEMENT} B ON ORG.fd_id = B.fd_parentid and B.fd_org_type ='8' and B.fd_is_available != 0
         |LEFT JOIN ${TableName.ODS_OA_ORG_PERSON} A ON A.fd_id = B.fd_id
         |""".stripMargin)

    val table = "ads_oa_staff"
    MysqlConnect.overrideTable(table,res)


  }
}

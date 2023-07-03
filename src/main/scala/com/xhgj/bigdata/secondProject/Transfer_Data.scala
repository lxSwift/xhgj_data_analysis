package com.xhgj.bigdata.secondProject
import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/6/30 11:37
 * @PackageName:com.xhgj.bigdata.secondProject
 * @ClassName: Transfer_Data
 * @Description: 调拨价数据分析
 * @Version 1.0
 */
object Transfer_Data {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job Transfer_Data.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession): Unit = {

    //结算组织=万聚国际（杭州）供应链有限公司或=杭州咸亨国际应急救援装备有限公司，且单据状态等于已审核未作废的应收单
    spark.sql(
      s"""
         |SELECT
         |  FDATE,
         |  FBILLNO,
         |  FCUSTOMERID,
         |  FID
         |FROM
         |  ${TableName.ODS_ERP_RECEIVABLE}
         |WHERE FSETTLEORGID IN ('1','481351') and FDOCUMENTSTATUS = 'C'
         |""".stripMargin).createOrReplaceTempView("t1")

    //销售订单明细先聚合,由于销售订单下面可能有相同物料,这个时候取最大的F_PAEZ_PRICE即可
    spark.sql(
      s"""
        |SELECT
        |OES.FID,
        |OES.FBILLNO
        |,OESE.FENTRYID
        |,OES.FPURTYPE
        |,OES.FSALEORGID,
        |OESE.FMATERIALID
        |,OESF.FPRICE
        |,OESE.FNOTE,
        |CAST(OESE.F_PAEZ_PRICE AS DECIMAL(18,2)) AS F_PAEZ_PRICE,
        |1+CAST(OESF.FTAXRATE AS DECIMAL(18,2))/100 AS FTAXRATE,
        |row_number() over(partition by OES.FBILLNO,OESE.FMATERIALID order by F_PAEZ_PRICE desc) as rownum
        |FROM ${TableName.ODS_ERP_SALORDER} OES
        |LEFT JOIN ${TableName.ODS_ERP_SALORDERENTRY} OESE ON OES.FID = OESE.FID
        |LEFT JOIN ${TableName.ODS_ERP_SALORDERENTRY_F} OESF ON OESE.FENTRYID = OESF.FENTRYID
        |""".stripMargin).createOrReplaceTempView("SALORDER_L")

    spark.sql(
      s"""
         |select
         |  *
         |from SALORDER_L
         |where rownum =1
         |""".stripMargin).createOrReplaceTempView("SALORDER")

//优先处理销售订单和应收单之间的值
    spark.sql(
      s"""
         |SELECT
         |  A.FDATE,--业务日期
         |  A.FBILLNO,--增值税发票号
         |  A_1.FPRICEQTY,--数量
         |  A_1.FTAXPRICE,--含税单价
         |  A_1.FPRICE AS FPRICE,--单价
         |  A_1.FENTRYTAXRATE,--税率(%)
         |  A_1.FTAXAMOUNTFOR,--税额
         |  A_1.FALLAMOUNTFOR,--含税金额
         |  A_1.FNOTAXAMOUNTFOR,--不含税金额
         |  A_1.FORDERNUMBER,--销售单订单号
         |  CASE
         |    WHEN L.FPURTYPE='B' OR L.FSALEORGID='554744' THEN nvl(L.FPRICE,0)
         |    WHEN L.FSALEORGID IN ('1','481351') AND NVL(L.FNOTE ,'') != '渠道中心' THEN cast(NVL(L.F_PAEZ_PRICE,0)/NVL(L.FTAXRATE,1) as decimal(18,2))
         |    WHEN L.FSALEORGID IN ('1','481351') AND NVL(L.FNOTE ,'') = '渠道中心' THEN nvl(A_1.FPRICE,0)
         |    ELSE 0 END AS TERMINALPRICE, --终端不含税单价
         |  A_1.FCOSTAMTSUM,--成本去税总金额
         |  A_1.FMATERIALID,
         |  A_1.FPRICEUNITID,
         |  A_1.F_PAEZ_BASE2,
         |  A_1.FPROJECTNO,
         |  A.FCUSTOMERID,
         |  L.FMATERIALID FMATERIALID_L
         |from t1 A
         |JOIN ${TableName.ODS_ERP_RECEIVABLEENTRY} A_1 ON A.FID = A_1.FID
         |LEFT JOIN SALORDER L ON A_1.FORDERNUMBER = L.FBILLNO AND A_1.FMATERIALID = L.FMATERIALID
         |""".stripMargin).createOrReplaceTempView("result")


//    val result = spark.sql(
//      s"""
//         |SELECT
//         |  FBILLNO,
//         |  FORDERNUMBER,FMATERIALID
//         |FROM result
//         |where rownum = 2
//         |""".stripMargin)
//      println("count="+result.count())
//      result.show(50)

    val result = spark.sql(
      s"""
         |SELECT
         |  A.FDATE,--业务日期
         |  A.FBILLNO,--增值税发票号
         |  B.fname CUSTOMERNAME,--客户
         |  "已审核" AS FDOCUMENTSTATUS,--单据状态
         |  C.fnumber MATERIALID,--物料编码
         |  D.FNAME BRANDNAME,--品牌
         |  C.fname MATERIALNAME,--商品名称
         |  C.FSPECIFICATION SPECIFICATION,--规格
         |  A.FPRICEQTY AS QTY,--数量
         |  E.FNAME UNITNAME,--单位
         |  A.FTAXPRICE AS TAXPRICE,--含税单价
         |  A.FPRICE AS PRICE,--单价
         |  A.FENTRYTAXRATE AS TAXRATE,--税率(%)
         |  A.FTAXAMOUNTFOR AS TAXAMOUNTFOR,--税额
         |  A.FALLAMOUNTFOR as ALLAMOUNTFOR,--含税金额
         |  A.FNOTAXAMOUNTFOR AS NOTAXAMOUNTFOR,--不含税金额
         |  F.FNAME AS KHNAME, --考核类别
         |  H.FNAME AS BUYERNAME, --采购员
         |  I.FNAME AS DEPARTMENTNAME, --采购部门
         |  J.FNAME AS SALENAME, --明细销售员
         |  K.F_PAEZ_TEXT AS SALCOMPANY, --销售员所属公司
         |  A.FORDERNUMBER AS ORDERNUMBER,--销售单订单号
         |  A.TERMINALPRICE AS TERMINALPRICE, --终端不含税单价
         |  cast(A.TERMINALPRICE*NVL(A.FPRICEQTY,0) as decimal(18,2)) AS TERMINALSUM,--终端不含税金额
         |  nvl(M.FNUMBER,'') AS PROJECTNUMBER,--项目编号
         |  nvl(M.FNAME,'') AS PROJECTNAME,--项目名称
         |  A.FCOSTAMTSUM AS COSTAMTSUM,--成本去税总金额
         |  CAST(A.FNOTAXAMOUNTFOR AS DECIMAL(18,2)) - CAST(A.FCOSTAMTSUM AS DECIMAL(18,2)) AS MAOLI--毛利
         |FROM result A
         |LEFT JOIN ${TableName.DIM_CUSTOMER} B ON A.FCUSTOMERID = B.fcustid
         |LEFT JOIN ${TableName.DIM_MATERIAL} C ON A.FMATERIALID = C.FMATERIALID
         |LEFT JOIN ${TableName.DIM_PAEZ_ENTRY100020} D ON C.F_PAEZ_BASE = D.FID
         |LEFT JOIN ${TableName.DIM_UNIT} E ON A.FPRICEUNITID = E.funitid
         |LEFT JOIN ${TableName.DIM_CUST100501} F ON C.f_khr = F.FID
         |LEFT JOIN ${TableName.ODS_ERP_MATERIALPURCHASE} G ON C.FMATERIALID = G.FMATERIALID
         |LEFT JOIN ${TableName.DIM_BUYER} H ON G.FPURCHASERID = H.FID
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} I ON C.F_PAEZ_BASE1 = I.fdeptid
         |LEFT JOIN ${TableName.DIM_SALEMAN} J ON A.F_PAEZ_BASE2 = J.FID
         |LEFT JOIN ${TableName.DIM_EMPINFO} K ON J.FNUMBER = K.FNUMBER
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} M ON A.FPROJECTNO = M.FID
         |""".stripMargin)


    println("result:" + result.count())
    val table = "ads_aat_transferdata"

    MysqlConnect.overrideTable(table,result)

  }

}

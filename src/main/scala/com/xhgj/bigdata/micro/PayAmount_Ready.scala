package com.xhgj.bigdata.micro

import com.xhgj.bigdata.util.{Config, MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

import java.sql.Types.VARCHAR
import java.util.Properties

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
//    应收单相关信息表更新， F_ORDERTYPE为1代表大票
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${TableName.DWS_RECE_PAYAMOUNT}
         |select
         |	b.F_PXDF_TEXT1 PRONAME,
         |	b.F_PXDF_TEXT PRONO,
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
         |left join DW_XHGJ.DIM_CUSTOMER cus on cus.fcustid=a.FCUSTOMERID
         |left join DW_XHGJ.DIM_SALEMAN sal on sal.fid=a.FSALEERID
         |where a.FDOCUMENTSTATUS='C' AND COALESCE(a.F_ORDERTYPE,'0') = '1'
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

    //应付单列表, 主表,单据状态只取已审核的数据以及结算组织为万聚的
    val result = spark.sql(
      s"""
         |select
         |  BT.FNAME FBILLTYPEID,--单据类型
         |  CASE
         |    WHEN PAY.FBUSINESSTYPE ='CG' THEN '普通采购'
         |    WHEN PAY.FBUSINESSTYPE ='FY' THEN '费用采购'
         |    WHEN PAY.FBUSINESSTYPE ='ZC' THEN '资产采购'
         |    ELSE '其他采购' END AS FBUSINESSTYPE,--业务类型
         |  PAY.FDATE,--业务日期
         |  SUP.FNAME FSUPPLIERNAME,--供应商
         |  PAY.FBILLNO,--增值税发票号
         |  CUR.FNAME FCURRENCY,--币别
         |  PAY.FALLAMOUNTFOR ALLAMOUNT,--价税合计_总
         |  PAY.FENDDATE,--到期日
         |  ORG.fname FSETTLEORG,--结算组织
         |  ORG2.FNAME FPURCHASEORG,--采购组织
         |  DEP.FNAME FPURCHASEDEPT,--采购部门
         |  BUY.FNAME FPURCHASERNAME,--采购员
         |  BUY.FNUMBER FPURCHASERNUMBER,--采购员编号
         |  "已审核" AS FDOCUMENTSTATUS,--单据状态
         |  ORG3.FNAME FPAYORGID,--付款组织
         |  MAT.FNUMBER FMATERIALID,--物料编号
         |  MAT.FSPECIFICATION,--规格型号
         |  MAT.FNAME FMATERIALIDNAME,--物料名称
         |  UNIT.fname FPRICEUNITID,--计价单位
         |  PAT_E.FPRICE,--单价
         |  PAT_E.FPRICEQTY,--计价数量
         |  PAT_E.FTAXPRICE,--含税单价
         |  PAT_E.FPRICEWITHTAX,--含税净价
         |  PAT_E.FENTRYTAXRATE,--税率
         |  PAT_E.FCOMMENT,--备注
         |  PAT_E.FENTRYDISCOUNTRATE,--折扣率
         |  PAT_E.FSOURCEBILLNO,--源单编号
         |  PAT_E.FORDERNUMBER,--采购订单号
         |  CASE
         |    WHEN PAT_E.FSOURCETYPE = 'PUR_MRB' THEN '采购退料单'
         |    WHEN PAT_E.FSOURCETYPE = 'PUR_InitMRS' THEN '期初采购退料单'
         |    WHEN PAT_E.FSOURCETYPE = 'PAEZ_SubContract' THEN '分包合同'
         |    WHEN PAT_E.FSOURCETYPE = 'STK_InitInStock' THEN '期初采购入库单'
         |    WHEN PAT_E.FSOURCETYPE = 'STK_InStock' THEN '采购入库单'
         |    WHEN PAT_E.FSOURCETYPE = 'AP_Payable' THEN '应付单'
         |    WHEN PAT_E.FSOURCETYPE = 'IOS_APSettlement' THEN '应付结算清单'
         |    WHEN PAT_E.FSOURCETYPE = 'PUR_PurchaseOrder' THEN '采购订单'
         |    ELSE '未知源单类型' END AS FSOURCETYPE,--源单类型
         |  PAT_E.FDISCOUNTAMOUNTFOR,--折扣额
         |  PAT_E.FNOTAXAMOUNTFOR,--不含税金额
         |  PAT_E.FTAXAMOUNTFOR,--税额
         |  PAT_E.FALLAMOUNTFOR,--价税合计_单物料
         |  PAT_E.FBASICUNITQTY,--计价基本数量
         |  LOT.FNAME FLOTNAME,--批号
         |  PAT_E.FTAXAMOUNT,--税额本位币
         |  PAT_E.FALLAMOUNT,--价税合计本位币
         |  PAT_E.FNOTAXAMOUNT,--不含税额本位币
         |  CASE
         |    WHEN PAT_O.FISFREE = '1' THEN '是'
         |    WHEN PAT_O.FISFREE = '0' THEN '否'
         |    ELSE '未知' END AS FISFREE,--是否赠品
         |  UNIT2.FNAME FLOTUNIT,--库存单位
         |  PAT_O.FSTOCKQTY FLOTQTY,--库存数量
         |  PAT_O.FSTOCKBASEQTY FLOTBASESENDQTY,--库存基本数量
         |  PAT_E.FPAYMENTAMOUNT,--已结算金额
         |  MAT.FDESCRIPTION,--描述
         |  ENT.FNAME F_PAEZ_BASE,--品牌
         |  STO.FNAME STOCKNAME,--仓库
         |  PAT_E.F_PXDF_TEXT SALORDERNUMBER,--销售单号
         |  SAL.FNAME SALEMAN ,--销售员
         |  SAL.fdeptname FDEPTNAME,--销售部门
         |  PRO.fnumber PROJECTNO,--项目编号
         |  PRO.FNAME PROJECTNAME,--项目名称
         |  PAT_E.F_PXDF_PRICE,--参考含税调拨价
         |  PAT_E.F_PXDF_PRICE1,--调拨单价差值
         |  ENTRY.FNAME CUST_ENTRY,--事业部
         |  PRO.fnumber F_PROJECTNO,--项目编码
         |  FLE.FNAME FLEX,--仓位
         |  PRO.FBEHALFINVOICERATIO DKBL,--代开比率
         |  CUST.FNAME khr,--考核类别
         |  row_number() over(ORDER BY PAY.FDATE) fid --主键id
         |from ${TableName.ODS_ERP_PAYABLE} PAY
         |JOIN ${TableName.ODS_ERP_PAYABLEENTRY} PAT_E ON PAT_E.FID = PAY.FID
         |LEFT JOIN ${TableName.ODS_ERP_PAYABLEENTRY_O} PAT_O ON PAT_E.fentryid = PAT_O.fentryid
         |LEFT JOIN ${TableName.DIM_BILLTYPE} BT ON PAY.FBILLTYPEID = BT.FBILLTYPEID
         |LEFT JOIN ${TableName.DIM_SUPPLIER} SUP ON PAY.FSUPPLIERID = SUP.fsupplierid
         |LEFT JOIN ${TableName.DIM_CURRENCY_ERP} CUR ON PAY.FCURRENCYID = CUR.fcurrencyid
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} ORG ON PAY.FSETTLEORGID = ORG.forgid
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} ORG2 ON PAY.FPURCHASEORGID = ORG2.forgid
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} DEP ON PAY.FPURCHASEDEPTID = DEP.fdeptid
         |LEFT JOIN ${TableName.DIM_BUYER} BUY ON PAY.FPURCHASERID = BUY.fid
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} ORG3 ON PAY.FPAYORGID = ORG3.forgid
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON PAT_E.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_UNIT} UNIT ON PAT_E.FPRICEUNITID = UNIT.funitid
         |LEFT JOIN ${TableName.DIM_LOTMASTER} LOT ON PAT_O.FLOT = LOT.flotid
         |LEFT JOIN ${TableName.DIM_UNIT} UNIT2 ON PAT_O.FSTOCKUNITID =UNIT2.funitid
         |LEFT JOIN ${TableName.DIM_PAEZ_ENTRY100020} ENT ON MAT.F_PAEZ_BASE = ENT.fid
         |LEFT JOIN ${TableName.DIM_STOCK} STO ON PAT_E.F_PAEZ_BASE = STO.fstockid
         |LEFT JOIN ${TableName.DIM_SALEMAN} SAL ON PAT_E.F_PXDF_BASE= SAL.fid
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} PRO ON PAT_E.F_PROJECTNO = PRO.fid
         |LEFT JOIN ${TableName.DIM_ENTRY100504} ENTRY ON PAT_E.F_PAEZ_BASE1 = ENTRY.fid
         |LEFT JOIN ${TableName.DIM_FLEXVALUESENTRY} FLE ON PAT_E.F_PAEZ_FLEX = FLE.FENTRYID
         |LEFT JOIN ${TableName.DIM_CUST100501} CUST ON CUST.fid = MAT.f_khr
         |WHERE PAY.FDOCUMENTSTATUS = 'C' and PAY.FSETTLEORGID = '1'
         |""".stripMargin)


    val table = "ads_srm_payable"
    val columnstr = "FBILLTYPEID varchar(255),FBUSINESSTYPE varchar(255),FDATE varchar(255),FSUPPLIERNAME varchar(255),FBILLNO varchar(255),FCURRENCY varchar(255),ALLAMOUNT varchar(255),FENDDATE varchar(255),FSETTLEORG varchar(255),FPURCHASEORG varchar(255),FPURCHASEDEPT varchar(255),FPURCHASERNAME varchar(255),FPURCHASERNUMBER varchar(255),FDOCUMENTSTATUS varchar(255),FPAYORGID varchar(255),FMATERIALID varchar(255),FSPECIFICATION varchar(255),FMATERIALIDNAME varchar(255),FPRICEUNITID varchar(255),FPRICE varchar(255),FPRICEQTY varchar(255),FTAXPRICE varchar(255),FPRICEWITHTAX varchar(255),FENTRYTAXRATE varchar(255),FCOMMENT varchar(255),FENTRYDISCOUNTRATE varchar(255),FSOURCEBILLNO varchar(255),FORDERNUMBER varchar(255),FSOURCETYPE varchar(255),FDISCOUNTAMOUNTFOR varchar(255),FNOTAXAMOUNTFOR varchar(255),FTAXAMOUNTFOR varchar(255),FALLAMOUNTFOR varchar(255),FBASICUNITQTY varchar(255),FLOTNAME varchar(255),FTAXAMOUNT varchar(255),FALLAMOUNT varchar(255),FNOTAXAMOUNT varchar(255),FISFREE varchar(255),FLOTUNIT varchar(255),FLOTQTY varchar(255),FLOTBASESENDQTY varchar(255),FPAYMENTAMOUNT varchar(255),FDESCRIPTION varchar(1000),F_PAEZ_BASE varchar(255),STOCKNAME varchar(255),SALORDERNUMBER varchar(255),SALEMAN varchar(255),FDEPTNAME varchar(255),PROJECTNO varchar(255),PROJECTNAME varchar(255),F_PXDF_PRICE varchar(255),F_PXDF_PRICE1 varchar(255),CUST_ENTRY varchar(255),F_PROJECTNO varchar(255),FLEX varchar(255),DKBL varchar(255),khr varchar(255),fid varchar(255)"
    val typestr = s"FBILLTYPEID=${VARCHAR},FBUSINESSTYPE=${VARCHAR},FDATE=${VARCHAR},FSUPPLIERNAME=${VARCHAR},FBILLNO=${VARCHAR},FCURRENCY=${VARCHAR},ALLAMOUNT=${VARCHAR},FENDDATE=${VARCHAR},FSETTLEORG=${VARCHAR},FPURCHASEORG=${VARCHAR},FPURCHASEDEPT=${VARCHAR},FPURCHASERNAME=${VARCHAR},FPURCHASERNUMBER=${VARCHAR},FDOCUMENTSTATUS=${VARCHAR},FPAYORGID=${VARCHAR},FMATERIALID=${VARCHAR},FSPECIFICATION=${VARCHAR},FMATERIALIDNAME=${VARCHAR},FPRICEUNITID=${VARCHAR},FPRICE=${VARCHAR},FPRICEQTY=${VARCHAR},FTAXPRICE=${VARCHAR},FPRICEWITHTAX=${VARCHAR},FENTRYTAXRATE=${VARCHAR},FCOMMENT=${VARCHAR},FENTRYDISCOUNTRATE=${VARCHAR},FSOURCEBILLNO=${VARCHAR},FORDERNUMBER=${VARCHAR},FSOURCETYPE=${VARCHAR},FDISCOUNTAMOUNTFOR=${VARCHAR},FNOTAXAMOUNTFOR=${VARCHAR},FTAXAMOUNTFOR=${VARCHAR},FALLAMOUNTFOR=${VARCHAR},FBASICUNITQTY=${VARCHAR},FLOTNAME=${VARCHAR},FTAXAMOUNT=${VARCHAR},FALLAMOUNT=${VARCHAR},FNOTAXAMOUNT=${VARCHAR},FISFREE=${VARCHAR},FLOTUNIT=${VARCHAR},FLOTQTY=${VARCHAR},FLOTBASESENDQTY=${VARCHAR},FPAYMENTAMOUNT=${VARCHAR},FDESCRIPTION=${VARCHAR},F_PAEZ_BASE=${VARCHAR},STOCKNAME=${VARCHAR},SALORDERNUMBER=${VARCHAR},SALEMAN=${VARCHAR},FDEPTNAME=${VARCHAR},PROJECTNO=${VARCHAR},PROJECTNAME=${VARCHAR},F_PXDF_PRICE=${VARCHAR},F_PXDF_PRICE1=${VARCHAR},CUST_ENTRY=${VARCHAR},F_PROJECTNO=${VARCHAR},FLEX=${VARCHAR},DKBL=${VARCHAR},khr=${VARCHAR},fid=${VARCHAR}"

    MysqlConnect.overrideTableDateType(table,result,columnstr,typestr)

  }
}

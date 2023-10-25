package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/10/24 10:31
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: Wanju_SalesReturn_Statistics
 * @Description: 万聚销售退货统计报表
 * @Version 1.0
 */
object WanjuSalesReturnStatistics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job WanjuSalesReturnStatistics.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //    salman(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession)={
    /**
     * 下方两个是主要是获取销售退货单相关信息
     */

    spark.sql(
      s"""
         |SELECT
         |  coalesce(big.F_PAEZ_TEXT1,DSO.F_paez_Text13,'') F_PAEZ_TEXT1,--销售员所属公司
         |  DSO.fbillno
         |FROM
         |${TableName.DWD_SAL_ORDER} DSO
         |left join ${TableName.DIM_PROJECTBASIC} dp on DSO.fprojectbasic  = dp.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on dp.fnumber = BIG.fbillno
         |GROUP BY  DSO.fbillno,coalesce(big.F_PAEZ_TEXT1,DSO.F_paez_Text13,'')
         |""".stripMargin).createOrReplaceTempView("salorder")

    //不含税；按物料编码和批号匹配；单价由采购入库单金额（本位币）/实收数量得出
    spark.sql(
      s"""
         |SELECT
         |  MAT.fnumber, --物料编码
         |  DL.fnumber FLOT, --批号
         |  if(MAX(oeie.FREALQTY) != '0' or trim(MAX(oeie.FREALQTY)) != '',MAX(oeif.FAMOUNT_LC) / MAX(oeie.FREALQTY),0) FCOSTPRICE_LC --不含税单价
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_F} oeif ON oeif.FENTRYID = oeie.FENTRYID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oeie.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oeie.FLOT = dl.FLOTID
         |group by MAT.fnumber,DL.fnumber
         |""".stripMargin).createOrReplaceTempView("INSTOCK")


    /**
     * 不含税；销售组织为“万聚国际（杭州）供应链有限公司”的，单价直接取销售退货单里的“单价”这个字段；销售组织非万聚的，
     * 根据销售退货单号+物料编号+批号+数量共4个条件到“应收结算清单-物料”中业务单据名称为“销售退货单”的明细中匹配“单价”这个字段
     */
    spark.sql(
      s"""
         |SELECT
         |  FPRICE,
         |  oead.FBASEQTY ,--基本单位数量
         |  FBIZBILLNO, --业务单据编号
         |  MAT.FNUMBER c_material_no,--物料编号
         |  LOT.fnumber c_flot_no  --批号
         |from ${TableName.ODS_ERP_ARSETTLEMENT} oea
         |left join ${TableName.ODS_ERP_ARSETTLEMENTDETAIL} oead on oea.fid = oead.fid
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oead.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} LOT ON oead.FLOT = LOT.flotid
         |where FBIZFORMID ='SAL_RETURNSTOCK'
         |""".stripMargin).createOrReplaceTempView("opo")

    //FSTOCKFLAG库存更新标识  为1 代表是   0代表否
    val result = spark.sql(
      s"""
         |SELECT
         |  OER.fbillno c_billno, --销售退货单编号
         |  OER.FAPPROVEDATE c_approve_date,--审核日期
         |  ORGA.fname c_sale_org,--销售组织
         |  CUS.FNAME c_return_customer,--退货客户
         |  MAT.FNUMBER c_material_no,--物料编号
         |  LOT.fnumber c_flot_no,--批号
         |  BUY.fnumber c_purchaser_no,--采购员编码
         |  BUY.fname c_purchaser,--采购员
         |  ORG.fname c_procure_department,--采购部门
         |  DSA.fnumber c_saleno, --销售员编码
         |  DSA.fname c_salename, --销售员
         |  DS.fname c_store,--仓库
         |  OERE.FREALQTY c_actual_returnqty,--实退数量
         |  OERE.F_PAEZ_TEXT1 c_return_reason , --退货原因
         |  OERE.F_PAEZ_TEXT2 c_notes, --备注
         |  case when OER.F_PAEZ_COMBO111 = '1' then '是'
         |   else '否' end as c_if_procedure , --是否走流程
         |  DSO.F_PAEZ_TEXT1 c_saler_company, --销售员所属公司
         |  INS.FCOSTPRICE_LC c_unit_costprice, --成本单价
         |  case when ORGA.fname = '万聚国际（杭州）供应链有限公司' then OEREF.FPRICE
         |    else opo.FPRICE end as c_sale_unitprice --单价
         |from
         |  ${TableName.ODS_ERP_RETURNSTOCK} OER
         |LEFT JOIN ${TableName.ODS_ERP_RETURNSTOCKENTRY} OERE ON OER.FID  = OERE.FID
         |LEFT JOIN ${TableName.ODS_ERP_RETURNSTOCKENTRY_F} OEREF ON OERE.FENTRYID  = OEREF.FENTRYID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON OERE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN salorder DSO ON OERE.F_PAEZ_TEXT = DSO.fbillno
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON OERE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_ORGANIZATIONS} ORGA ON OER.FSALEORGID = ORGA.forgid
         |LEFT JOIN ${TableName.DIM_CUSTOMER} CUS ON OER.FRETCUSTID = CUS.FCUSTID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} LOT ON OERE.FLOT = LOT.flotid
         |LEFT JOIN ${TableName.DIM_BUYER} BUY ON OERE.F_PAEZ_BASE = BUY.fid
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} ORG ON OERE.F_PXDF_Base1 = ORG.fdeptid
         |LEFT JOIN ${TableName.DIM_SALEMAN} DSA ON OER.FSALESMANID = DSA.FID
         |LEFT JOIN INSTOCK INS ON MAT.FNUMBER = INS.fnumber and LOT.fnumber = INS.FLOT
         |left join opo on OER.fbillno = opo.FBIZBILLNO and MAT.FNUMBER=opo.c_material_no and LOT.fnumber = opo.c_flot_no and OERE.FREALQTY = opo.FBASEQTY
         |WHERE OER.FSTOCKORGID IN ('1') AND OER.FDOCUMENTSTATUS = 'C' AND OERE.FSTOCKFLAG = '1'
         | AND DS.fname IN ('直销库','海宁1号库','嘉峪关分仓','惠州分仓')
         |""".stripMargin)


    val table = "ads_wanju_salesreturn"

    MysqlConnect.overrideTable(table,result)

  }
}

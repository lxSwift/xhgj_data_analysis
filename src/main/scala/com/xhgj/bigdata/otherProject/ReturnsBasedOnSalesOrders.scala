package com.xhgj.bigdata.otherProject

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/9/26 9:04
 * @PackageName:com.xhgj.bigdata.otherProject
 * @ClassName: ReturnsBasedOnSalesOrders
 * @Description: 以销定采的退货情况
 * @Version 1.0
 */
object ReturnsBasedOnSalesOrders {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job ReturnsBasedOnSalesOrders.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //    salman(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession): Unit = {
    /**
     * 下方两个是主要是获取销售退货单相关信息
     */

    spark.sql(
      s"""
         |SELECT
         |  big.F_PAEZ_TEXT1,--销售员所属公司
         |  DSO.fbillno
         |FROM
         |${TableName.DWD_SAL_ORDER} DSO
         |left join ${TableName.DIM_PROJECTBASIC} dp on DSO.fprojectbasic  = dp.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on dp.fnumber = BIG.fbillno
         |GROUP BY  DSO.fbillno,big.F_PAEZ_TEXT1
         |""".stripMargin).createOrReplaceTempView("salorder")

    spark.sql(
      s"""
         |SELECT
         |  OER.fbillno c_billno, --销售退货单编号
         |  OER.FAPPROVEDATE c_approve_date,--审核日期
         |  MAT.FNUMBER c_material_no,--物料编号
         |  LOT.fnumber c_flot_no,--批号
         |  OERE.FREALQTY c_actual_returnqty,--实退数量
         |  BUY.fnumber c_purchaser_no,--采购员编码
         |  BUY.fname c_purchaser,--采购员
         |  ORG.fname c_procure_department,--采购部门
         |  DSA.fnumber c_saleno, --销售员编码
         |  DSA.fname c_salename, --销售员
         |  DSO.F_PAEZ_TEXT1 c_saler_company --销售员所属公司
         |from
         |  ${TableName.ODS_ERP_RETURNSTOCK} OER
         |LEFT JOIN ${TableName.ODS_ERP_RETURNSTOCKFIN} OERF ON OER.FID = OERF.FID
         |LEFT JOIN ${TableName.ODS_ERP_RETURNSTOCKENTRY} OERE ON OER.FID  = OERE.FID and OERE.F_PAEZ_TEXT2 NOT LIKE '%流程%' and OERE.F_PAEZ_TEXT2 NOT LIKE '%调整%'
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON OER.F_PAEZ_BASE3 = DS.FSTOCKID
         |LEFT JOIN salorder DSO ON OERE.F_PAEZ_TEXT = DSO.fbillno
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON OERE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} LOT ON OERE.FLOT = LOT.flotid
         |LEFT JOIN ${TableName.DIM_BUYER} BUY ON OERE.F_PAEZ_BASE = BUY.fid
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} ORG ON OERE.F_PXDF_Base1 = ORG.fdeptid
         |LEFT JOIN ${TableName.DIM_SALEMAN} DSA ON OER.FSALESMANID = DSA.FID
         |WHERE OER.FSTOCKORGID IN ('1','481351') AND OER.FDOCUMENTSTATUS = 'C' AND OERF.FISGENFORIOS = '0'
         | AND DS.fname IN ('海宁1号库','应急海宁1号库','应急海宁2号库','嘉峪关分仓','惠州分仓')
         |""".stripMargin).createOrReplaceTempView("RETURN")


    //获取订单编号为主键,查看其是否存在申请单 大于0即存在
    spark.sql(
      s"""
         |SELECT
         |  FBILLNO,
         |  FENTRYID,
         |  FID,
         |  count(if(trim(FSRCBILLNO) = '' or substring(trim(FSRCBILLNO),1,4) != 'CGSQ' ,null,FSRCBILLNO)) reqnum
         |FROM
         |  ${TableName.DWD_PUR_POORDER} DPP
         |group by fbillno,FID,FENTRYID
         |""".stripMargin).createOrReplaceTempView("pooder")

  //选出以销定采的数据
    spark.sql(
      s"""
         |SELECT
         |  MAT.fnumber, --物料编码
         |  DL.fnumber FLOT, --批号
         |  if(MAX(oeif.FCOSTPRICE_LC) is null or MAX(trim(oeif.FCOSTPRICE_LC)) = '', if(MAX(oeie.FREALQTY) != '0' or trim(MAX(oeie.FREALQTY)) != '',MAX(oeif.FAMOUNT_LC) / MAX(oeie.FREALQTY),0) ,MAX(oeif.FCOSTPRICE_LC)) FCOSTPRICE_LC --不含税单价
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_F} oeif ON oeif.FENTRYID = oeie.FENTRYID
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_LK} OEIL ON OEIE.FENTRYID = OEIL.FENTRYID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oeie.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oeie.FLOT = dl.FLOTID
         |JOIN pooder DPP ON DPP.reqnum > 0 and OEIL.FSBILLID = DPP.FID and OEIL.FSID = DPP.FENTRYID
         |WHERE
         |oei.FSTOCKORGID IN ('1','481351')
         |AND oei.FDOCUMENTSTATUS = 'C' and MAT.fnumber is not null and trim(MAT.fnumber) !='' and DL.fnumber is not null and trim(DL.fnumber) !=''
         |group by MAT.fnumber,DL.fnumber
         |""".stripMargin).createOrReplaceTempView("YXDC")

    //获取销售退货单中以销定采的数据
    val result = spark.sql(
      s"""
         |SELECT
         |RE.c_billno, --销售退货单编号
         |RE.c_approve_date,--审核日期
         |RE.c_material_no,--物料编号
         |RE.c_flot_no,--批号
         |RE.c_actual_returnqty,--实退数量
         |RE.c_purchaser_no,--采购员编码
         |RE.c_purchaser,--采购员
         |RE.c_procure_department,--采购部门
         |YX.FCOSTPRICE_LC c_unit_costprice,--成本单价
         |RE.c_actual_returnqty * YX.FCOSTPRICE_LC c_return_amount,--销售退回金额
         |RE.c_saleno, --销售员编码
         |RE.c_salename, --销售员
         |RE.c_saler_company --销售员所属公司
         |FROM
         |  RETURN RE JOIN YXDC YX ON RE.c_material_no = YX.fnumber and RE.c_flot_no = YX.FLOT
         |""".stripMargin)

    val table1 = "ads_pur_salereturns"
    MysqlConnect.overrideTable(table1,result)

    result.createOrReplaceTempView("tmpres")

    spark.sql(
      s"""
         |SELECT
         |  c_material_no,
         |  c_flot_no,
         |  SUM(c_actual_returnqty) c_actual_returnqty,
         |  MAX(c_unit_costprice) c_unit_costprice,
         |  MIN(c_approve_date) c_approve_date
         |FROM tmpres
         |GROUP BY c_material_no,c_flot_no
         |""".stripMargin).createOrReplaceTempView("res1")


    //ERP-采购退货单
    spark.sql(
      s"""
         |SELECT
         |  MRBE.FRMREALQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot,
         |  MRB.FCREATEDATE
         |FROM
         |${TableName.ODS_ERP_MRB_DA} MRB
         |JOIN ${TableName.ODS_ERP_MRBENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE MRB.FSTOCKORGID IN ('1','481351') AND MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','应急海宁1号库','应急海宁2号库','嘉峪关分仓','惠州分仓')
         |""".stripMargin).createOrReplaceTempView("pur_return")


    //ERP-销售出库单列表
    spark.sql(
      s"""
         |SELECT
         |  MRBE.FREALQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot,
         |  MRB.FAPPROVEDATE
         |FROM
         |${TableName.ODS_ERP_OUTSTOCK} MRB
         |JOIN ${TableName.ODS_ERP_OUTSTOCKENTRY} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE MRB.FSTOCKORGID IN ('1','481351') AND MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','应急海宁1号库','应急海宁2号库','嘉峪关分仓','惠州分仓')
         |""".stripMargin).createOrReplaceTempView("sal_out")

    //ERP-即时库存
    spark.sql(
      s"""
         |SELECT
         |  MRB.FBASEQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot
         |FROM
         |${TableName.ODS_ERP_INVENTORY_DA} MRB
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRB.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRB.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRB.FLOT = dl.FLOTID
         |WHERE MRB.FSTOCKORGID IN ('1','481351')
         |AND DS.fname IN ('海宁1号库','应急海宁1号库','应急海宁2号库','嘉峪关分仓','惠州分仓')
         |""".stripMargin).createOrReplaceTempView("Instant")



    val result2 = spark.sql(
      s"""
         |SELECT
         |RE.c_material_no,--物料编号
         |RE.c_flot_no,--批号
         |MAX(RE.c_actual_returnqty) c_actual_returnqty,--实退数量
         |MAX(RE.c_unit_costprice) c_unit_costprice,--成本单价
         |MAX(RE.c_actual_returnqty)*MAX(RE.c_unit_costprice) c_return_amount,--销售退回金额
         |SUM(PR.FRMREALQTY) c_actual_returnqty_cgth,--采购退出数量
         |ifnull(SUM(PR.FRMREALQTY),0)*MAX(RE.c_unit_costprice) c_return_amount_cgth,--采购退出金额
         |sum(SO.FREALQTY) c_secondary_salesqty ,--二次销售数量
         |ifnull(sum(SO.FREALQTY),0)*MAX(RE.c_unit_costprice) c_secondary_salesprice,--二次销售金额
         |sum(INS.FBASEQTY) c_stock_quantity,--在库数量
         |ifnull(sum(INS.FBASEQTY),0)*MAX(RE.c_unit_costprice) c_stock_price --在库金额
         |from
         |  res1 RE LEFT JOIN pur_return PR ON RE.c_material_no = PR.FNUMBER AND RE.c_flot_no = PR.flot AND RE.c_approve_date <= PR.FCREATEDATE
         |LEFT JOIN sal_out SO ON RE.c_material_no = SO.FNUMBER AND RE.c_flot_no = SO.flot AND RE.c_approve_date <= SO.FAPPROVEDATE
         |LEFT JOIN Instant INS ON RE.c_material_no = INS.FNUMBER AND RE.c_flot_no = INS.flot
         |GROUP BY c_material_no,c_flot_no
         |""".stripMargin)

    val table2 = "ads_pur_salereturnstatus"
    MysqlConnect.overrideTable(table2,result2)



  }
}

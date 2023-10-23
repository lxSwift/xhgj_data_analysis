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
         |  coalesce(big.F_PAEZ_TEXT1,DSO.F_paez_Text13,'') F_PAEZ_TEXT1,--销售员所属公司
         |  DSO.fbillno
         |FROM
         |${TableName.DWD_SAL_ORDER} DSO
         |left join ${TableName.DIM_PROJECTBASIC} dp on DSO.fprojectbasic  = dp.fid
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on dp.fnumber = BIG.fbillno
         |GROUP BY  DSO.fbillno,coalesce(big.F_PAEZ_TEXT1,DSO.F_paez_Text13,'')
         |""".stripMargin).createOrReplaceTempView("salorder")

    //FSTOCKFLAG库存更新标识  为1 代表是   0代表否
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
         |LEFT JOIN ${TableName.ODS_ERP_RETURNSTOCKENTRY} OERE ON OER.FID  = OERE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON OERE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN salorder DSO ON OERE.F_PAEZ_TEXT = DSO.fbillno
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON OERE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} LOT ON OERE.FLOT = LOT.flotid
         |LEFT JOIN ${TableName.DIM_BUYER} BUY ON OERE.F_PAEZ_BASE = BUY.fid
         |LEFT JOIN ${TableName.DIM_DEPARTMENT} ORG ON OERE.F_PXDF_Base1 = ORG.fdeptid
         |LEFT JOIN ${TableName.DIM_SALEMAN} DSA ON OER.FSALESMANID = DSA.FID
         |WHERE OER.FSTOCKORGID IN ('1') AND OER.FDOCUMENTSTATUS = 'C' AND OERE.FSTOCKFLAG = '1'
         | AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓')
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
         |  if(MAX(oeie.FREALQTY) != '0' or trim(MAX(oeie.FREALQTY)) != '',MAX(oeif.FAMOUNT_LC) / MAX(oeie.FREALQTY),0) FCOSTPRICE_LC --不含税单价
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_F} oeif ON oeif.FENTRYID = oeie.FENTRYID
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_LK} OEIL ON OEIE.FENTRYID = OEIL.FENTRYID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oeie.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oeie.FLOT = dl.FLOTID
         |JOIN pooder DPP ON DPP.reqnum > 0 and OEIL.FSBILLID = DPP.FID and OEIL.FSID = DPP.FENTRYID
         |WHERE
         |oei.FSTOCKORGID IN ('1')
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


    //ERP-采购退货单  退料组织和需求组织不一致的是内部交易单据 库存更新标识   0代表否  1代表是  取1
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
         |WHERE MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓') and MRBE.FSTOCKFLAG='1'
         |""".stripMargin).createOrReplaceTempView("pur_return")


    //ERP-销售出库单列表  FSTOCKFLAG 库存更新标识 0代表否  1代表是
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
         |WHERE MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓') AND MRBE.FSTOCKFLAG = '1'
         |""".stripMargin).createOrReplaceTempView("sal_out")


    //采购入库单
    spark.sql(
      s"""
         |SELECT
         |  MAT.fnumber, --物料编码
         |  DL.fnumber FLOT, --批号
         |  sum(oeie.FREALQTY) FREALQTY --实收数量
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oeie.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oeie.FLOT = dl.FLOTID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON oeie.FSTOCKID = DS.FSTOCKID
         |WHERE
         |oei.FDOCUMENTSTATUS = 'C' and MAT.fnumber is not null and trim(MAT.fnumber) !='' and DL.fnumber is not null and trim(DL.fnumber) !=''
         |AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓') and oeie.FSTOCKFLAG='1'
         |group by MAT.fnumber,DL.fnumber
         |""".stripMargin).createOrReplaceTempView("instock")


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
         |WHERE DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓')
         |""".stripMargin).createOrReplaceTempView("Instant")

    //直接调拨单列表(调出数量)  FSTOCKOUTFLAG调出库存更新标识
    spark.sql(
      s"""
         |SELECT
         |  sum(MRBE.FQTY) FQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot
         |FROM
         |${TableName.ODS_ERP_STKTRANSFERIN_DA} MRB
         |JOIN ${TableName.ODS_ERP_STKTRANSFERINENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSRCSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FSRCMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE  MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓') and MRBE.FSTOCKOUTFLAG = '1'
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("stktransferin_out")

    //直接调拨单列表(调入数量)  FSTOCKINFLAG调入库存更新标识
    spark.sql(
      s"""
         |SELECT
         |  sum(MRBE.FQTY) FQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot
         |FROM
         |${TableName.ODS_ERP_STKTRANSFERIN_DA} MRB
         |JOIN ${TableName.ODS_ERP_STKTRANSFERINENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FDESTSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FSRCMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE  MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓') and MRBE.FSTOCKINFLAG = '1'
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("stktransferin_IN")


    //盘亏单列表
    spark.sql(
      s"""
         |SELECT
         |  sum(MRBE.FLOSSQTY) FQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot
         |FROM
         |${TableName.ODS_ERP_STKCOUNTLOSS_DA} MRB
         |JOIN ${TableName.ODS_ERP_STKCOUNTLOSSENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE  MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓')
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("stkcountloss")

    //盘盈单列表
    spark.sql(
      s"""
         |SELECT
         |  sum(MRBE.FGAINQTY) FQTY,
         |  MAT.FNUMBER,
         |  dl.fnumber flot
         |FROM
         |${TableName.ODS_ERP_STKCOUNTGAIN_DA} MRB
         |JOIN ${TableName.ODS_ERP_STKCOUNTGAINENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE  MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓')
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("stkcountgain")
    //组装拆卸表(组装) 子表编码
    spark.sql(
      s"""
         |SELECT
         |	MAT.FNUMBER,
         |	dl.fnumber flot,
         |	SUM(C.FQTY) FQTY
         |FROM ${TableName.ODS_ERP_ASSEMBLY_DA} A
         |left join ${TableName.ODS_ERP_ASSEMBLYPRODUCT_DA} B ON A.FID=B.FID
         |LEFT JOIN ${TableName.ODS_ERP_ASSEMBLYSUBITEM_DA} C ON B.FENTRYID = C.fentryid
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON C.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON C.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON C.FLOT = dl.FLOTID
         |WHERE A.FAFFAIRTYPE = 'Assembly' and A.FDOCUMENTSTATUS = 'C' AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓')
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("ASSEMBLY")

    //组装拆卸表(组装) 主表编码
    spark.sql(
      s"""
         |SELECT
         |	MAT.FNUMBER,
         |	dl.fnumber flot,
         |	SUM(B.FQTY) FQTY
         |FROM ${TableName.ODS_ERP_ASSEMBLY_DA} A
         |left join ${TableName.ODS_ERP_ASSEMBLYPRODUCT_DA} B ON A.FID=B.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON B.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON B.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON B.FLOT = dl.FLOTID
         |WHERE A.FAFFAIRTYPE = 'Assembly' and A.FDOCUMENTSTATUS = 'C' AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓')
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("ASSEMBLY_M")

    //组装拆卸表(拆卸)子表编码
    spark.sql(
      s"""
         |SELECT
         |	MAT.FNUMBER,
         |	dl.fnumber flot,
         |	SUM(C.FQTY) FQTY
         |FROM ${TableName.ODS_ERP_ASSEMBLY_DA} A
         |left join ${TableName.ODS_ERP_ASSEMBLYPRODUCT_DA} B ON A.FID=B.FID
         |LEFT JOIN ${TableName.ODS_ERP_ASSEMBLYSUBITEM_DA} C ON B.FENTRYID = C.fentryid
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON C.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON C.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON C.FLOT = dl.FLOTID
         |WHERE A.FAFFAIRTYPE = 'Dassembly' and A.FDOCUMENTSTATUS = 'C' AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓')
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("DEASSEMBLY")

    //组装拆卸表(拆卸)子表编码
    spark.sql(
      s"""
         |SELECT
         |	MAT.FNUMBER,
         |	dl.fnumber flot,
         |	SUM(B.FQTY) FQTY
         |FROM ${TableName.ODS_ERP_ASSEMBLY_DA} A
         |left join ${TableName.ODS_ERP_ASSEMBLYPRODUCT_DA} B ON A.FID=B.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON B.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON B.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON B.FLOT = dl.FLOTID
         |WHERE A.FAFFAIRTYPE = 'Dassembly' and A.FDOCUMENTSTATUS = 'C' AND DS.fname IN ('海宁1号库','嘉峪关分仓','惠州分仓')
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("DEASSEMBLY_M")


    spark.sql(
      s"""
         |SELECT
         |  RE.c_material_no,--物料编号
         |  RE.c_flot_no,--批号
         |  MAX(RE.c_actual_returnqty) c_actual_returnqty,--实退数量
         |  MAX(RE.c_unit_costprice) c_unit_costprice,--成本单价
         |  SUM(PR.FRMREALQTY) c_actual_returnqty_cgth--采购退货数量
         |from
         |res1 RE LEFT JOIN pur_return PR ON RE.c_material_no = PR.FNUMBER AND RE.c_flot_no = PR.flot
         |GROUP BY c_material_no,c_flot_no
         |""".stripMargin).createOrReplaceTempView("res_no1")

    spark.sql(
      s"""
         |SELECT
         |  RE.c_material_no,--物料编号
         |  RE.c_flot_no,--批号
         |  MAX(RE.c_actual_returnqty) c_actual_returnqty,--实退数量
         |  MAX(RE.c_unit_costprice) c_unit_costprice,--成本单价
         |  MAX(RE.c_actual_returnqty_cgth) c_actual_returnqty_cgth,--采购退货数量
         |  sum(SO.FREALQTY) c_secondary_salesqty --二次销售数量
         |from
         |res_no1 RE
         |LEFT JOIN sal_out SO ON RE.c_material_no = SO.FNUMBER AND RE.c_flot_no = SO.flot
         |group by RE.c_material_no,RE.c_flot_no
         |""".stripMargin).createOrReplaceTempView("res_no2")

    spark.sql(
      s"""
         |SELECT
         |  RE.c_material_no,--物料编号
         |  RE.c_flot_no,--批号
         |  MAX(RE.c_actual_returnqty) c_actual_returnqty,--实退数量
         |  MAX(RE.c_unit_costprice) c_unit_costprice,--成本单价
         |  MAX(RE.c_actual_returnqty_cgth) c_actual_returnqty_cgth,--采购退货数量
         |  MAX(RE.c_secondary_salesqty) c_secondary_salesqty,--二次销售数量
         |  sum(INS.FBASEQTY) c_stock_quantity--在库数量
         |from
         |  res_no2 RE
         |LEFT JOIN Instant INS ON RE.c_material_no = INS.FNUMBER AND RE.c_flot_no = INS.flot
         |GROUP BY c_material_no,c_flot_no
         |""".stripMargin).createOrReplaceTempView("res_no3")

    spark.sql(
      s"""
         |SELECT
         |  RE.c_material_no,--物料编号
         |  RE.c_flot_no,--批号
         |  MAX(RE.c_actual_returnqty) c_actual_returnqty,--实退数量
         |  MAX(RE.c_unit_costprice) c_unit_costprice,--成本单价
         |  MAX(RE.c_actual_returnqty_cgth) c_actual_returnqty_cgth,--采购退货数量
         |  MAX(RE.c_secondary_salesqty) c_secondary_salesqty,--二次销售数量
         |  MAX(RE.c_stock_quantity) c_stock_quantity, --在库数量
         |  sum(OCK.FREALQTY) c_instock_qty--入库数量
         |from
         |  res_no3 RE
         |LEFT JOIN instock OCK ON RE.c_material_no = OCK.FNUMBER AND RE.c_flot_no = OCK.flot
         |GROUP BY c_material_no,c_flot_no
         |""".stripMargin).createOrReplaceTempView("res_no4")

    val result2 = spark.sql(
      s"""
         |SELECT
         |  RE.c_material_no,--物料编号
         |  RE.c_flot_no,--批号
         |  RE.c_actual_returnqty,--实退数量
         |  RE.c_unit_costprice,--成本单价
         |  RE.c_actual_returnqty_cgth,--采购退货数量
         |  RE.c_secondary_salesqty,--二次销售数量
         |  RE.c_stock_quantity, --在库数量
         |  RE.c_instock_qty,--入库数量
         |  STK.FQTY c_transferout_qty, --调拨出库数量
         |  STKI.FQTY c_transferin_qty, --调拨入库数量
         |  STKLOSS.FQTY c_inventoryloss_qty, --盘亏数量
         |  STKGAIN.FQTY c_inventorygain_qty, --盘盈数量
         |  ass.FQTY c_assembly_qty, --组装数量
         |  deass.FQTY c_deassembly_qty, --拆卸数量
         |  assm.FQTY c_assembly_qty_m, --主表组装数量
         |  deassm.FQTY c_deassembly_qty_m --主表拆卸数量
         |from
         |  res_no4 RE
         |LEFT JOIN stktransferin_out STK ON RE.c_material_no = STK.FNUMBER AND RE.c_flot_no = STK.flot
         |LEFT JOIN stktransferin_IN STKI ON RE.c_material_no = STKI.FNUMBER AND RE.c_flot_no = STKI.flot
         |LEFT JOIN stkcountloss STKLOSS ON RE.c_material_no = STKLOSS.FNUMBER AND RE.c_flot_no = STKLOSS.flot
         |LEFT JOIN stkcountgain STKGAIN ON RE.c_material_no = STKGAIN.FNUMBER AND RE.c_flot_no = STKGAIN.flot
         |LEFT JOIN ASSEMBLY ass ON RE.c_material_no = ass.FNUMBER AND RE.c_flot_no = ass.flot
         |LEFT JOIN DEASSEMBLY deass ON RE.c_material_no = deass.FNUMBER AND RE.c_flot_no = deass.flot
         |LEFT JOIN ASSEMBLY_M assm ON RE.c_material_no = assm.FNUMBER AND RE.c_flot_no = assm.flot
         |LEFT JOIN DEASSEMBLY_M deassm ON RE.c_material_no = deassm.FNUMBER AND RE.c_flot_no = deassm.flot
         |""".stripMargin)


    val table2 = "ads_pur_salereturnstatus"
    MysqlConnect.overrideTable(table2,result2)



  }
}

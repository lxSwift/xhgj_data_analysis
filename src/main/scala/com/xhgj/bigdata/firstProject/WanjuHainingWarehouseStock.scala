package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.{MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/10/25 14:24
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: WanjuHainingWarehouseStock
 * @Description: 万聚海宁库库存报表
 * @Version 1.0
 */
object WanjuHainingWarehouseStock {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job WanjuHainingWarehouseStock.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //    salman(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession): Unit = {

    //采购入库
    spark.sql(
      s"""
         |SELECT
         |  MAT.fnumber c_material_no, --物料编码
         |  DL.fnumber c_flot_no, --批号
         |  min(oei.FAPPROVEDATE) c_approve_date --审核日期
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oeie.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oeie.FLOT = dl.FLOTID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON oeie.FSTOCKID = DS.FSTOCKID
         |WHERE
         |oei.FDOCUMENTSTATUS = 'C' AND DS.fname = '海宁1号库' and oeie.FSTOCKFLAG='1'
         |group by MAT.fnumber,DL.fnumber
         |""".stripMargin).createOrReplaceTempView("INSTOCK1")

    //销售退货单
    spark.sql(
      s"""
         |SELECT
         |  min(OER.FAPPROVEDATE) c_approve_date,--审核日期
         |  MAT.FNUMBER c_material_no,--物料编号
         |  LOT.fnumber c_flot_no --批号
         |from
         |  ${TableName.ODS_ERP_RETURNSTOCK} OER
         |LEFT JOIN ${TableName.ODS_ERP_RETURNSTOCKENTRY} OERE ON OER.FID  = OERE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON OERE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON OERE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} LOT ON OERE.FLOT = LOT.flotid
         |WHERE  OER.FDOCUMENTSTATUS = 'C' AND OERE.FSTOCKFLAG = '1' AND DS.fname ='海宁1号库'
         |group by MAT.fnumber,LOT.fnumber
         |""".stripMargin).createOrReplaceTempView("RETURNSTOCK")

    //ERP-直接调拨单列表(取调入仓库) 库存调入标识为是
    spark.sql(
      s"""
         |SELECT
         |  min(MRB.FAPPROVEDATE) c_approve_date,--审核日期
         |  MAT.FNUMBER c_material_no,
         |  dl.fnumber c_flot_no
         |FROM
         |${TableName.ODS_ERP_STKTRANSFERIN_DA} MRB
         |JOIN ${TableName.ODS_ERP_STKTRANSFERINENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FDESTSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FSRCMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE  MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname = '海宁1号库' and MRBE.FSTOCKINFLAG = '1'
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("STKTRANSFERIN")

    //组装拆卸表(组装) 主表编码
    spark.sql(
      s"""
         |SELECT
         |	MAT.FNUMBER c_material_no,
         |	dl.fnumber c_flot_no,
         |	min(A.FAPPROVEDATE) c_approve_date --审核日期
         |FROM ${TableName.ODS_ERP_ASSEMBLY_DA} A
         |left join ${TableName.ODS_ERP_ASSEMBLYPRODUCT_DA} B ON A.FID=B.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON B.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON B.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON B.FLOT = dl.FLOTID
         |WHERE A.FAFFAIRTYPE = 'Assembly' and A.FDOCUMENTSTATUS = 'C' AND DS.fname = '海宁1号库'
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("ASSEMBLY_M")

    //组装拆卸表(拆卸)子表编码
    spark.sql(
      s"""
         |SELECT
         |	MAT.FNUMBER c_material_no,
         |	dl.fnumber c_flot_no,
         |	min(A.FAPPROVEDATE) c_approve_date --审核日期
         |FROM ${TableName.ODS_ERP_ASSEMBLY_DA} A
         |left join ${TableName.ODS_ERP_ASSEMBLYPRODUCT_DA} B ON A.FID=B.FID
         |LEFT JOIN ${TableName.ODS_ERP_ASSEMBLYSUBITEM_DA} C ON B.FENTRYID = C.fentryid
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON C.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON C.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON C.FLOT = dl.FLOTID
         |WHERE A.FAFFAIRTYPE = 'Dassembly' and A.FDOCUMENTSTATUS = 'C' AND DS.fname = '海宁1号库'
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("DEASSEMBLY")

    //盘盈单列表
    spark.sql(
      s"""
         |SELECT
         |  min(MRB.FAPPROVEDATE) c_approve_date, --审核日期
         |  MAT.FNUMBER c_material_no,
         |  dl.fnumber c_flot_no
         |FROM
         |${TableName.ODS_ERP_STKCOUNTGAIN_DA} MRB
         |JOIN ${TableName.ODS_ERP_STKCOUNTGAINENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE  MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname = '海宁1号库'
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("stkcountgain")

    //初始库存表
    spark.sql(
      s"""
         |SELECT
         |  min(MRB.FAPPROVEDATE) c_approve_date, --审核日期
         |  MAT.FNUMBER c_material_no,
         |  dl.fnumber c_flot_no
         |FROM
         |${TableName.ODS_ERP_STKINVINIT_DA} MRB
         |JOIN ${TableName.ODS_ERP_STKINVINITDETAIL_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE  MRB.FDOCUMENTSTATUS = 'C'
         |AND DS.fname = '海宁1号库'
         |GROUP BY MAT.fnumber,dl.fnumber
         |""".stripMargin).createOrReplaceTempView("STKINVINIT")


    //采购入库  最早审核日期入库单据的供应商
    spark.sql(
      s"""
         |select
         |c_material_no,
         |c_flot_no,
         |c_supplier_name
         |from(
         |SELECT
         |  MAT.fnumber c_material_no, --物料编码
         |  DL.fnumber c_flot_no, --批号
         |  DS.FNAME c_supplier_name, --供应商
         |  row_number() over(partition by MAT.fnumber,DL.fnumber order by oei.FAPPROVEDATE asc) nums
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oeie.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oeie.FLOT = dl.FLOTID
         |LEFT JOIN ${TableName.DIM_SUPPLIER} DS ON oei.FSUPPLIERID = DS.FSUPPLIERID
         |WHERE
         |oei.FDOCUMENTSTATUS = 'C'  and oeie.FSTOCKFLAG='1' and oei.FSTOCKORGID = '1'
         |) aoo where nums = 1
         |""".stripMargin).createOrReplaceTempView("INSTOCK2")


    //采购入库  最晚时间的单价
    spark.sql(
      s"""
         |select
         |c_material_no,
         |c_flot_no,
         |if(FREALQTY != '0' or trim(FREALQTY) != '',FAMOUNT_LC / FREALQTY,0) FCOSTPRICE_LC --不含税单价
         |from(
         |SELECT
         |  MAT.fnumber c_material_no, --物料编码
         |  DL.fnumber c_flot_no, --批号
         |  oeie.FREALQTY,
         |  oeif.FAMOUNT_LC,
         |  row_number() over(partition by MAT.fnumber,DL.fnumber order by oei.FAPPROVEDATE desc) nums
         |FROM ${TableName.ODS_ERP_INSTOCK} oei
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY} oeie ON oei.FID = oeie.FID
         |LEFT JOIN ${TableName.ODS_ERP_INSTOCKENTRY_F} oeif ON oeie.FENTRYID = oeif.FENTRYID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON oeie.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON oeie.FLOT = dl.FLOTID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON oeie.FSTOCKID = DS.FSTOCKID
         |WHERE
         |oei.FDOCUMENTSTATUS = 'C'  and oeie.FSTOCKFLAG='1' and oei.FSTOCKORGID = '1'
         |) aoo where nums = 1
         |""".stripMargin).createOrReplaceTempView("INSTOCK3")

    //组装拆卸表(组装) 主表编码     单价=成品表总成本/成品表数量；同一物料+批号的单价只能有1个；有多个的，取最迟单据
    spark.sql(
      s"""
         |select
         |c_material_no,
         |c_flot_no,
         |if(FQTY != '0' or trim(FQTY) != '',FAMOUNT / FQTY,0) FCOSTPRICE_LC --不含税单价
         |from
         |(
         |SELECT
         |	MAT.FNUMBER c_material_no,
         |	dl.fnumber c_flot_no,
         |  B.FAMOUNT,
         |  B.FQTY,
         |	row_number() over(partition by MAT.fnumber,dl.fnumber order by A.FAPPROVEDATE desc) nums
         |FROM ${TableName.ODS_ERP_ASSEMBLY_DA} A
         |left join ${TableName.ODS_ERP_ASSEMBLYPRODUCT_DA} B ON A.FID=B.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON B.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON B.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON B.FLOT = dl.FLOTID
         |WHERE A.FAFFAIRTYPE = 'Assembly' and A.FDOCUMENTSTATUS = 'C' and A.FSTOCKORGID ='1'
         |) aoo where nums = 1
         |""".stripMargin).createOrReplaceTempView("ASSEMBLY_M2")


    //组装拆卸表(拆卸)子表编码  单价=子件表总成本/子件表数量；同一物料+批号的单价只能有1个；有多个的，取最迟单据
    spark.sql(
      s"""
         |select
         |c_material_no,
         |c_flot_no,
         |if(FQTY != '0' or trim(FQTY) != '',FAMOUNT / FQTY,0) FCOSTPRICE_LC --不含税单价
         |from
         |(
         |SELECT
         |	MAT.FNUMBER c_material_no,
         |	dl.fnumber c_flot_no,
         |  C.FAMOUNT,
         |  C.FQTY,
         |	row_number() over(partition by MAT.fnumber,dl.fnumber order by A.FAPPROVEDATE desc) nums
         |FROM ${TableName.ODS_ERP_ASSEMBLY_DA} A
         |left join ${TableName.ODS_ERP_ASSEMBLYPRODUCT_DA} B ON A.FID=B.FID
         |LEFT JOIN ${TableName.ODS_ERP_ASSEMBLYSUBITEM_DA} C ON B.FENTRYID = C.fentryid
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON C.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON C.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON C.FLOT = dl.FLOTID
         |WHERE A.FAFFAIRTYPE = 'Dassembly' and A.FDOCUMENTSTATUS = 'C' and A.FSTOCKORGID ='1'
         |) aoo where nums = 1
         |""".stripMargin).createOrReplaceTempView("DEASSEMBLY2")

    //盘盈单列表
    spark.sql(
      s"""
         |select
         |c_material_no,
         |c_flot_no,
         |FPRICE FCOSTPRICE_LC
         |from
         |(
         |SELECT
         |  MAT.FNUMBER c_material_no,
         |  dl.fnumber c_flot_no,
         |  FPRICE,
         |  row_number() over(partition by MAT.FNUMBER,dl.fnumber order by MRB.FAPPROVEDATE desc) nums
         |FROM
         |${TableName.ODS_ERP_STKCOUNTGAIN_DA} MRB
         |JOIN ${TableName.ODS_ERP_STKCOUNTGAINENTRY_DA} MRBE ON MRB.FID = MRBE.FID
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRBE.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRBE.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRBE.FLOT = dl.FLOTID
         |WHERE  MRB.FDOCUMENTSTATUS = 'C'
         |AND MRB.FSTOCKORGID = '1'
         |) aoo where nums = 1
         |""".stripMargin).createOrReplaceTempView("stkcountgain2")


    //获取mpm表的数据
    spark.sql(
      s"""
         |select
         |  ch.c_code mcode, --物料（编码）
         |  nvl(em.c_code,'') purerid,--采购员（编码）
         |  nvl(em.c_name,'') purername,--采购员（名称）
         |  nvl(de.c_name,'') cainame,--采购部门（名称）
         |  nvl(ec.c_name,'') examinename --考核类别（名称）
         |from
         |  ODS_XHGJ.ODS_MPM_CHILD ch
         |left join ODS_XHGJ.ODS_MPM_EXAMINE_CATEGORY ec on ch.c_examine_category = ec.c_code
         |left join ODS_XHGJ.ODS_MPM_EMP em on ch.c_purchase_man = em.id
         |left join ODS_XHGJ.ODS_MPM_DEPT de on ch.c_purchase_dept = de.id
         |WHERE (ch.c_category_two <> 'ea8ebef464c5c8c90164ca9e2bac0015'
         |	OR ch.c_category_two IS NULL) and ch.c_code != ''
         |""".stripMargin).createOrReplaceTempView("mpmmaterial")


    spark.sql(
      s"""
         |SELECT
         |  TKE.FSUPPLYINTERID ,
         |  SUM(COALESCE(TKE.FBASEQTY, 0)) AS FBASELOCKQTY ,
         |  SUM(COALESCE(TKE.FSECQTY, 0)) AS FSECLOCKQTY
         |FROM ${TableName.ODS_ERP_RESERVELINKENTRY_DA} TKE
         |WHERE TKE.FSUPPLYFORMID = 'STK_Inventory' AND TKE.FBASEQTY > 0
         |GROUP BY TKE.FSUPPLYINTERID
         |""".stripMargin).createOrReplaceTempView("RESERVELINKENTRY")
    //最终结果
    spark.sql(
      s"""
         |SELECT
         |  MAT.FNUMBER c_material_code,--物料编码
         |  ENT.FNAME c_material_brand,--品牌
         |  MAT.FNAME c_material_name,--物料名称
         |  MAT.FSPECIFICATION c_specification, --规格型号
         |  dl.fnumber c_flot_no,--批号
         |  mpm.examinename c_assessment_category,--考核类别
         |  mpm.purerid c_purchaser_id,--采购员（编码）
         |  mpm.purername c_purchaser_name, --采购员（名称）
         |  mpm.cainame c_procure_department,--采购部门
         |  CASE WHEN d.FSTOREURNUM=0 THEN MRB.FBASEQTY
         |  ELSE MRB.FBASEQTY*d.FSTOREURNOM/d.FSTOREURNUM END AS c_stock_num ,--库存量(主单位)
         |  CASE WHEN d.FSTOREURNUM=0 THEN (MRB.FBASEQTY - COALESCE(b.FBASELOCKQTY, 0))
         |  ELSE (MRB.FBASEQTY - COALESCE(b.FBASELOCKQTY, 0))*d.FSTOREURNOM/d.FSTOREURNUM END AS c_available_num ,--可用量(主单位)
         |  ins.c_supplier_name --供应商
         |FROM
         |${TableName.ODS_ERP_INVENTORY_DA} MRB
         |LEFT JOIN ${TableName.DIM_STOCK} DS ON MRB.FSTOCKID = DS.FSTOCKID
         |LEFT JOIN ${TableName.DIM_MATERIAL} MAT ON MRB.FMATERIALID = MAT.FMATERIALID
         |LEFT JOIN ${TableName.DIM_PAEZ_ENTRY100020} ENT ON MAT.F_PAEZ_BASE=ENT.fid
         |LEFT JOIN ${TableName.DIM_LOTMASTER} dl ON MRB.FLOT = dl.FLOTID
         |LEFT JOIN ${TableName.ODS_ERP_MATERIALSTOCK_DA} d ON MRB.FMATERIALID=d.FMATERIALID
         |LEFT JOIN RESERVELINKENTRY b ON b.FSUPPLYINTERID = MRB.FID
         |left join mpmmaterial mpm ON MAT.FNUMBER = mpm.mcode
         |left join INSTOCK2 ins on MAT.FNUMBER = ins.c_material_no and dl.fnumber = ins.c_flot_no
         |WHERE DS.fname = '海宁1号库'
         |""".stripMargin).createOrReplaceTempView("res1")

    spark.sql(
      s"""
         |SELECT
         |  A.c_material_code,--物料编码
         |  A.c_material_brand,--品牌
         |  A.c_material_name,--物料名称
         |  A.c_specification, --规格型号
         |  A.c_flot_no,--批号
         |  A.c_assessment_category,--考核类别
         |  A.c_purchaser_id,--采购员（编码）
         |  A.c_purchaser_name, --采购员（名称）
         |  A.c_procure_department,--采购部门
         |  A.c_stock_num,--库存量(主单位)
         |  A.c_available_num, --可用量(主单位)
         |  LEAST(ins.c_approve_date,ret.c_approve_date,stk.c_approve_date,ass.c_approve_date,dea.c_approve_date,aga.c_approve_date,INV.c_approve_date) c_approve_date, --审核日期
         |  A.c_supplier_name
         |FROM
         |  res1 A
         |left join INSTOCK1 ins on A.c_material_code = ins.c_material_no and A.c_flot_no = ins.c_flot_no
         |left join RETURNSTOCK ret on A.c_material_code = ret.c_material_no and A.c_flot_no = ret.c_flot_no
         |left join STKTRANSFERIN stk on A.c_material_code = stk.c_material_no and A.c_flot_no = stk.c_flot_no
         |left join ASSEMBLY_M ass on A.c_material_code = ass.c_material_no and A.c_flot_no = ass.c_flot_no
         |left join DEASSEMBLY dea on A.c_material_code = dea.c_material_no and A.c_flot_no = dea.c_flot_no
         |left join stkcountgain aga on A.c_material_code = aga.c_material_no and A.c_flot_no = aga.c_flot_no
         |left join STKINVINIT INV on A.c_material_code = INV.c_material_no and A.c_flot_no = INV.c_flot_no
         |""".stripMargin).createOrReplaceTempView("res2")

    val result = spark.sql(
      s"""
         |SELECT
         |  A.c_material_code,--物料编码
         |  A.c_material_brand,--品牌
         |  A.c_material_name,--物料名称
         |  A.c_specification, --规格型号
         |  A.c_flot_no,--批号
         |  A.c_assessment_category,--考核类别
         |  A.c_purchaser_id,--采购员（编码）
         |  A.c_purchaser_name, --采购员（名称）
         |  A.c_procure_department,--采购部门
         |  cast(A.c_stock_num as int) c_stock_num,--库存量(主单位)
         |  cast(A.c_available_num as int) c_available_num, --可用量(主单位)
         |  A.c_approve_date, --审核日期
         |  A.c_supplier_name,--供应商
         |  case
         |    when coalesce(ins.FCOSTPRICE_LC,'') != '' then ins.FCOSTPRICE_LC
         |    when coalesce(ret.FCOSTPRICE_LC,'') != '' then ret.FCOSTPRICE_LC
         |    when coalesce(ass.FCOSTPRICE_LC,'') != '' then ass.FCOSTPRICE_LC
         |    when coalesce(stk.FCOSTPRICE_LC,'') != '' then stk.FCOSTPRICE_LC
         |    else 0 end as c_unit_costprice --成本单价
         |from
         |  res2 A
         |left join INSTOCK3 ins on A.c_material_code = ins.c_material_no and A.c_flot_no = ins.c_flot_no
         |left join DEASSEMBLY2 ret on A.c_material_code = ret.c_material_no and A.c_flot_no = ret.c_flot_no
         |left join ASSEMBLY_M2 ass on A.c_material_code = ass.c_material_no and A.c_flot_no = ass.c_flot_no
         |left join stkcountgain2 stk on A.c_material_code = stk.c_material_no and A.c_flot_no = stk.c_flot_no
         |where cast(A.c_stock_num as int) != 0 or cast(A.c_available_num as int) != 0
         |""".stripMargin)

    val tableName = "ads_wanju_warehousestock"

    MysqlConnect.overrideTable(tableName,result)


  }


}

package com.xhgj.bigdata.otherProject

import com.xhgj.bigdata.util.{Config, MysqlConnect}
import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @Author luoxin
 * @Date 2023/6/21 13:48
 * @PackageName:com.xhgj.bigdata.otherProject
 * @ClassName: Material
 * @Description: 物料表的建立
 * @Version 1.0
 */
object Material {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job Material.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)

    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession): Unit = {

    //先对项目这个字段进行加工(原字段是'、25、59、64、' 需要对应到项目表获得到)
    spark.sql(
      s"""
         |SELECT
         |  c.id id,
         |  CONCAT_WS('/', COLLECT_LIST(p.c_name)) AS platname
         |FROM ODS_XHGJ.ODS_MPM_CHILD c
         |left JOIN (
         |  SELECT c_code, c_name FROM ODS_XHGJ.ODS_MPM_PLATFORM
         |) p
         |ON array_contains(split(c.c_platform_code, '、'), p.c_code)
         |GROUP BY c.id
         |""".stripMargin).createOrReplaceTempView("plat")

    //三级类目
    spark.sql(
      s"""
         |SELECT
         |  id,
         |  category_id,
         |  c_erp_code,
         |  c_name
         |FROM ODS_XHGJ.ods_mpm_category
         |WHERE c_level = '3'
         |""".stripMargin).createOrReplaceTempView("category3")

    //二级类目
    spark.sql(
      s"""
         |SELECT
         |  id,
         |  category_id,
         |  c_erp_code,
         |  c_name
         |FROM ODS_XHGJ.ods_mpm_category
         |WHERE c_level = '2'
         |""".stripMargin).createOrReplaceTempView("category2")

    //一级类目
    spark.sql(
      s"""
         |SELECT
         |  id,
         |  category_id,
         |  c_erp_code,
         |  c_name
         |FROM ODS_XHGJ.ods_mpm_category
         |WHERE c_level = '1'
         |""".stripMargin).createOrReplaceTempView("category1")

    //对物料表进行列裁剪,物料主表通过c_isdisabled这个条件过滤掉 NULL和0都是不禁用，1禁用;以及c_state为正常;二级类目不为项目型专区的值;
    val res =spark.sql(
      s"""
         |select
         |  ch.c_code mcode, --物料（编码）
         |  nvl(cg.c_erp_code,'') codefcode, --四级类目（编码）
         |  nvl(cg.c_name,'') codefname, --四级类目（名称）
         |  nvl(cg3.c_erp_code,'') codemcode3, --三类目（类目）
         |  nvl(cg3.c_name,'') codemname3, --三类目（名称）
         |  nvl(cg2.c_erp_code,'') codemcode2, --俩类目（类目）
         |  nvl(cg2.c_name,'') codemname2, --俩类目（名称）
         |  nvl(cg1.c_erp_code,'') codemcode1, --一类目（类目）
         |  nvl(cg1.c_name,'') codemname1, --一类目（名称）
         |  ch.c_shipper shipper, --货主（编码）
         |  ch.c_name mname, --物料（名称）
         |  ch.c_manucode manucode, --型号规格
         |  ch.c_bar_code bar_code, --条码
         |  ch.c_mnemonic_code mnemonic_code, --助记码
         |  ch.c_supplier_code supplier_code,--供应商编码
         |  p.platname platname, --项目
         |  ch.c_description description, --描述
         |  ch.c_old_code old_code, --旧物料编码
         |  ch.c_price_m price_m, --市场价
         |  ch.c_price price, --商城价
         |  ch.c_transfer_price transfer_price, --调拨价
         |  bd.c_erp_code erp_code, --物料品牌（编码）
         |  CONCAT_WS('/',nvl(bd.c_name_en,''),nvl(bd.c_name_cn,'')) erp_name, --物料品牌（名称）
         |  un.c_name unname, --基本单位（名称）
         |  ch.c_inner_remark inner_remark, --描述（内部备注）
         |  case
         |    when ch.c_shelfstate = '1' then '上架'
         |    when ch.c_shelfstate = '0' then '下架'
         |    else '未知' end as shelfstate, --商城上架
         |  case
         |    when ch.c_isSale = '0' then '可售'
         |    when ch.c_isSale = '1' then '不可售'
         |    else '未知' end as isSale, --商城可售
         |  case
         |    when ch.c_tax_rate = 'SL01_SYS' then '17%增值税'
         |    when ch.c_tax_rate = 'SL02_SYS' then '13%增值税'
         |    when ch.c_tax_rate = 'SL03_SYS' then '7%增值税'
         |    when ch.c_tax_rate = 'SL04_SYS' then '0%增值税'
         |    when ch.c_tax_rate = 'SL05_SYS' then '11%增值税'
         |    when ch.c_tax_rate = 'SL06_SYS' then '6%增值税'
         |    when ch.c_tax_rate = 'SL07_SYS' then '3%增值税'
         |    when ch.c_tax_rate = 'SL08_SYS' then '16%增值税'
         |    when ch.c_tax_rate = 'SL09_SYS' then '1%增值税'
         |    when ch.c_tax_rate = 'SL45_SYS' then '12%增值税'
         |    when ch.c_tax_rate = 'SL62_SYS' then '10%增值税'
         |    when ch.c_tax_rate = '0004' then '9%增值税'
         |    when ch.c_tax_rate = '0002' then '服务费'
         |    else '未知' end as tax_rate, --默认税率（名称)
         |  nvl(ec.c_name,'') examinename, --考核类别（名称）
         |  ch.c_examine_category examine_category, --考核类别（编码）
         |  nvl(de.c_code,'') ccode,--采购部门（编码）
         |  nvl(de.c_name,'') cainame,--采购部门（名称）
         |  nvl(em.c_code,'') purerid,--采购员（编码）
         |  nvl(em.c_name,'') purername,--采购员（名称）
         |  nvl(em2.c_code,'') checkcode,--考核人（编码）
         |  nvl(em2.c_name,'') checkname,--考核人（名称）
         |  ch.c_gross_weight gross_weight,--毛重
         |  ch.c_weight weight,--净重
         |  split(ch.c_size,'、')[0] long,--长
         |  split(ch.c_size,'、')[1] wide,--宽
         |  split(ch.c_size,'、')[2] high,--高
         |  case
         |    when ch.c_content <>'' and ch.c_content is not null then '是'
         |    else '否' end as content, --是否有图
         |  case
         |    when nvl(em3.id,'') <> '' then em3.c_name
         |    else ch.c_create_man_name end as crname, --创建人（名称）
         |  case
         |    when ch.c_data_nature='1' then '存量'
         |    when ch.c_data_nature='2' then '增量'
         |    when ch.c_data_nature='3' then '已去存'
         |    else '未知' end as nature, --物料性质
         |  case
         |    when ch.c_isdisabled='1' then '是'
         |    when ch.c_isdisabled='0' then '否'
         |    else '未知' end as disabled , --是否禁用
         |  case
         |    when ch.c_warehouse_affect_price='1' then '是'
         |    when ch.c_warehouse_affect_price='0' then '否'
         |    else '未知' end as affect , --仓库影响价格
         |  ch.c_warehouse_affect_cost affect_cost, --仓库影响成本
         |  nvl(man.c_name,'') proname, --产品经理
         |  ch.c_moq moq, --起订量
         |  c_delivery_day delivery_day,  --发货期
         |  case
         |    when ch.c_type = '1' then '主推商品属性组'
         |    when ch.c_type = '2' then '常规商品属性组'
         |    when ch.c_type = '3' then '零星商品属性组'
         |    when ch.c_type = '0' then '未分类商品属性组'
         |    else '未知' end as type, --物料等级
         |  split(c_external_link,'、')[0] link1, --外部链接1（匹配）
         |  split(c_external_link,'、')[1] link2, --外部链接2（匹配）
         |  split(c_external_link,'、')[2] link3, --外部链接3（匹配）
         |  split(c_external_link,'、')[3] link4 --外部链接4（匹配）
         |from
         |  ODS_XHGJ.ODS_MPM_CHILD ch
         |left join ODS_XHGJ.ODS_MPM_CATEGORY cg on ch.category_id = cg.id
         |left join category3 cg3 on cg.category_id = cg3.id
         |left join category2 cg2 on cg3.category_id = cg2.id
         |left join category1 cg1 on cg2.category_id = cg1.id
         |left join plat p on ch.id = p.id
         |left join ODS_XHGJ.ODS_MPM_BRAND bd on ch.brand_id=bd.id
         |left join ODS_XHGJ.ODS_MPM_UNIT un on ch.c_unit = un.c_unit
         |left join ODS_XHGJ.ODS_MPM_EXAMINE_CATEGORY ec on ch.c_examine_category = ec.c_code
         |left join ODS_XHGJ.ODS_MPM_DEPT de on ch.c_purchase_dept = de.id
         |left join ODS_XHGJ.ODS_MPM_EMP em on ch.c_purchase_man = em.id
         |left join ODS_XHGJ.ODS_MPM_EMP em2 on ch.c_examiner = em2.id
         |left join ODS_XHGJ.ODS_MPM_EMP em3 on ch.c_create_man = em3.id
         |left join ODS_XHGJ.ODS_MPM_PRODUCT_MANAGER man on ch.product_manager_id = man.id
         |WHERE ch.c_category_two <> 'ea8ebef464c5c8c90164ca9e2bac0015'
         |	OR ch.c_category_two IS NULL
         |""".stripMargin)

    val table = "ads_oth_material"

    MysqlConnect.overrideTable(table, res)
  }

}

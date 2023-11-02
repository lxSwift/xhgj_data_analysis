package com.xhgj.bigdata.otherProject

import com.xhgj.bigdata.util.{AddressAnalysis, MysqlConnect, TableName}
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/9/13 9:58
 * @PackageName:com.xhgj.bigdata.otherProject
 * @ClassName: PipeNetworkOrder
 * @Description: 官网客户订单数据
 * @Version 1.0
 */
object PipeNetworkOrder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job PipeNetworkOrder.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //    salman(spark)
    //关闭SparkSession
    spark.stop()
  }

  def runRES(spark: SparkSession): Unit = {

    spark.udf.register("address_get",(str:String) =>addressget(str))
    //读取手工表在2022年的数据
    spark.sql(
      s"""
         |SELECT
         |  c_order_time,
         |  c_order_id,
         |  c_platform_price,
         |  c_title,
         |  c_receiving_province
         |FROM
         |  ${TableName.DWD_WRITE_PFORDER}
         |""".stripMargin).createOrReplaceTempView("write_pforder")

    //将履约平台的2023之后的数据取出来, 同一个订单类型里面的客户订单号是唯一的,c_state代表的是正常使用的数据, 46代表是管网的数据
    //订单状态待取消'3',取消'4',预占'6',全部订单'0'
    spark.sql(
      s"""
         |SELECT
         |  case when substring(A.c_docking_order_no,1,1) = '2' then A.c_order_time
         |  else IF(A.c_examine_time is not null and substring(A.c_examine_time,1,2) != '19',A.c_examine_time,A.c_order_time) end as c_order_time, --下单时间
         |  A.c_docking_order_no c_order_id, --客户订单号
         |  cast(A.c_platform_price as decimal(19,2)) - sum(cast(COALESCE(c_return_num,0) as decimal(19,2)) * cast(COALESCE(c_xs_price,0) as decimal(19,2))) c_platform_price, --平台总价
         |  A.c_title, --发票抬头
         |  address_get(A.c_address) c_receiving_province --收货省区
         |FROM
         |  ${TableName.ODS_OMS_PFORDER} A
         |LEFT JOIN ${TableName.ODS_OMS_PFRETURNORDER} B ON A.id = B.order_id and B.c_state = '1'
         |LEFT JOIN ${TableName.ODS_OMS_PFRETURNORDERDETAIL} C ON B.id = C.return_order_id and C.c_state = '1'
         |where A.c_state = '1' and A.c_docking_type IN ('46','ZQ001') and substring(A.c_order_time,1,4) >= '2023' and A.c_order_state not in('3','4','6','0')
         |group by A.c_docking_order_no,A.c_title,A.c_order_time,A.c_address,A.c_platform_price,A.c_examine_time
         |""".stripMargin).createOrReplaceTempView("oms_pforder")
    //合并上方两个数据
    spark.sql(
      s"""
         |SELECT
         |  *
         |FROM
         |  write_pforder
         |UNION ALL
         |SELECT
         |  *
         |FROM
         |  oms_pforder
         |""".stripMargin).createOrReplaceTempView("pforder")

    MysqlConnect.getMysqlData("write_pipenetwork_ordermapping",spark).createOrReplaceTempView("mysqldata")

    //从大票项目单取值
    spark.sql(
      s"""
         |SELECT
         |	subquery.CUSTOMERORDERID FCUSTOMERORDERID, --客户订单号
         |	subquery.fbillno, --项目编码
         |  subquery.FCREATEDATE CREATEDATE, --创建日期
         |	sum(bige.FPRICETAXAMOUNT) FPRICETAXAMOUNT,  --价税合计(含税)
         |	sum(bige.FAMOUNT) FAMOUNT, --金额(不含税)
         |	DS.FNAME SALENAME --销售员
         |FROM (
         |  SELECT CUSTOMERORDERID, fbillno,fid,FSALESMAN,FCREATEDATE
         |  FROM ${TableName.ODS_ERP_BIGTICKETPROJECT} big
         |  LATERAL VIEW explode(split(REPLACE(coalesce(FCUSTOMERORDERID,''), '\\'', ''),'、')) exploded AS CUSTOMERORDERID
         |  WHERE big.fprojectname IN('浙20221277-国家管网集团2022年电商平台采购项目','国家管网集团2022年电商平台采购项目')
         |) subquery
         |JOIN ${TableName.ODS_ERP_BigTicketProjectEntry} bige on subquery.fid = bige.fid
         |LEFT JOIN ${TableName.DIM_SALEMAN} DS ON subquery.FSALESMAN = DS.FID
         |group by subquery.fbillno,DS.FNAME,subquery.CUSTOMERORDERID,subquery.FCREATEDATE
         |""".stripMargin).createOrReplaceTempView("bigproject2")


    spark.sql(
      s"""
         |SELECT
         |  coalesce(my.c_customer_order,big.FCUSTOMERORDERID,'') FCUSTOMERORDERID, --客户订单号
         |  big.fbillno, --项目编码
         |	big.FPRICETAXAMOUNT,  --价税合计(含税)
         |	big.FAMOUNT, --金额(不含税)
         |  big.CREATEDATE, --创建日期
         |	big.SALENAME --销售员
         |FROM
         |bigproject2 big
         |left join mysqldata my on big.fbillno = my.c_project_order
         |""".stripMargin).createOrReplaceTempView("bigproject")


    //小票组织：咸亨国际科技股份有限公司、武汉咸亨国际能源科技有限公司；单据状态：已审核；
    // 项目编号与大票项目单信息中的项目编号相匹配的单据之汇总金额；金额应当除以代开比例，代开比例为空的按100计
    spark.sql(
      s"""
         |select
         |	DP.fnumber PRONO,
         | CAST(SUM(case when DP.FBEHALFINVOICERATIO is not null and TRIM(DP.FBEHALFINVOICERATIO) != '' and TRIM(DP.FBEHALFINVOICERATIO) != 0 then OERE.FPRICEQTY * OERE.FPRICE/DP.FBEHALFINVOICERATIO*100
         | else OERE.FPRICEQTY * OERE.FPRICE end) AS DECIMAL(19,2)) AS SALEAMOUNT , --不含税金额
         | CAST(SUM(case when DP.FBEHALFINVOICERATIO is not null and TRIM(DP.FBEHALFINVOICERATIO) != '' and TRIM(DP.FBEHALFINVOICERATIO) != 0 then OERE.FPRICEQTY * OERE.FTAXPRICE/DP.FBEHALFINVOICERATIO*100
         | else OERE.FPRICEQTY * OERE.FTAXPRICE end) AS DECIMAL(19,2)) AS SALETAXAMOUNT , --含税金额
         | sum(OERE.FCOSTAMTSUM) FCOSTAMTSUM --成本去税总金额
         |from
         |	${TableName.ODS_ERP_RECEIVABLE} a
         |join ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE on a.fid=OERE.fid
         |left join ${TableName.DIM_PROJECTBASIC} DP on OERE.FPROJECTNO=DP.fid
         |left join ${TableName.DIM_ORGANIZATIONS} org on org.forgid=a.FSETTLEORGID
         |where a.FDOCUMENTSTATUS='C' AND org.fname in ('咸亨国际科技股份有限公司','武汉咸亨国际能源科技有限公司')
         |group by DP.fnumber
         |""".stripMargin).createOrReplaceTempView("smallrece")



    //大票组织：DP咸亨国际科技股份有限公司；单据状态：已审核；项目编号与大票项目单信息中的项目编号相匹配的单据之汇总金额
    spark.sql(
      s"""
         |select
         |	DP.fnumber PRONO,
         | CAST(SUM(OERE.FPRICEQTY * OERE.FTAXPRICE) AS DECIMAL(19,2)) AS SALETAXAMOUNT --含税金额
         |from
         |	${TableName.ODS_ERP_RECEIVABLE} a
         |join ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE on a.fid=OERE.fid
         |left join ${TableName.DIM_PROJECTBASIC} DP on OERE.FPROJECTNO=DP.fid
         |left join ${TableName.DIM_ORGANIZATIONS} org on org.forgid=a.FSETTLEORGID
         |where a.FDOCUMENTSTATUS='C' AND org.fname ='DP咸亨国际科技股份有限公司'
         |group by DP.fnumber
         |""".stripMargin).createOrReplaceTempView("bigrece")

    //收款单和收款退款单
    spark.sql(
      s"""
         |SELECT DP.FNUMBER,SUM(OERE.FRECAMOUNTFOR_E) REAMOUNT
         |FROM ${TableName.ODS_ERP_RECEIVEBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVEBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.DIM_ORGANIZATIONS} org on org.forgid=OER.FPAYORGID
         |WHERE OER.FDOCUMENTSTATUS='C' AND org.fname ='DP咸亨国际科技股份有限公司'
         |GROUP BY DP.FNUMBER
         |""".stripMargin).createOrReplaceTempView("payment")

    spark.sql(
      s"""
         |SELECT DP.FNUMBER,SUM(OERE.FREALREFUNDAMOUNTFOR) AS REAMOUNT
         |FROM ${TableName.ODS_ERP_REFUNDBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_REFUNDBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.DIM_ORGANIZATIONS} org on org.forgid=OER.FPAYORGID
         |WHERE OER.FDOCUMENTSTATUS='C' AND org.fname ='DP咸亨国际科技股份有限公司'
         |GROUP BY DP.FNUMBER
         |""".stripMargin).createOrReplaceTempView("refund")

    spark.sql(
      s"""
         |select
         |	DP.fnumber PRONO,
         | CAST(SUM(OERE.FPRICEQTY * OERE.FTAXPRICE) AS INT) AS SALETAXAMOUNT --含税金额
         |from
         |	${TableName.ODS_ERP_RECEIVABLE} a
         |join ${TableName.ODS_ERP_RECEIVABLEENTRY} OERE on a.fid=OERE.fid
         |left join ${TableName.DIM_PROJECTBASIC} DP on OERE.FPROJECTNO=DP.fid
         |left join ${TableName.DIM_ORGANIZATIONS} org on org.forgid=a.FSETTLEORGID
         |where a.FDOCUMENTSTATUS='C' AND org.fname ='DP咸亨国际科技股份有限公司'
         |group by DP.fnumber
         |having SALETAXAMOUNT = 0
         |""".stripMargin).createOrReplaceTempView("need1")

    //最终汇总

    val result = spark.sql(
      s"""
         |select
         |  A.c_order_time,
         |  A.c_order_id,
         |  A.c_platform_price,
         |  A.c_title,
         |  A.c_receiving_province,
         |  B.fbillno c_project_no,
         |  B.FPRICETAXAMOUNT c_fpricetax_amount,
         |  B.FAMOUNT c_FAMOUNT,
         |  B.SALENAME c_SALENAME,
         |  B.CREATEDATE c_CREATEDATE,
         |  C.SALETAXAMOUNT c_saletaxamount,
         |  C.SALEAMOUNT c_saleamount,
         |  C.FCOSTAMTSUM c_fcostamtsum,
         |  D.SALETAXAMOUNT c_SALETAXAMOUNT_big,
         |  E.REAMOUNT c_payment_AMOUNT,
         |  F.REAMOUNT c_refund_amount
         |from
         |  pforder A
         |full join bigproject B on A.c_order_id = B.FCUSTOMERORDERID
         |left join smallrece C on B.fbillno = C.PRONO
         |left join bigrece D on B.fbillno = D.PRONO
         |left join payment E on B.fbillno = E.FNUMBER
         |left join refund F on B.fbillno = F.FNUMBER
         |left join need1 nn on B.fbillno = nn.PRONO
         |where nn.PRONO is null
         |""".stripMargin)

    print("num=========" + result.count())
    val table = "ads_oth_pipenetwork"
    MysqlConnect.overrideTable(table, result)

  }


  def addressget(infoadd: String) = {
    AddressAnalysis.provincesMatch(infoadd)
  }
}

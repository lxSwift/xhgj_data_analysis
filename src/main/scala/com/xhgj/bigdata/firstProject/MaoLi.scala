package com.xhgj.bigdata.firstProject

import com.xhgj.bigdata.util.TableName
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author luoxin
 * @Date 2023/4/27 16:07
 * @PackageName:com.xhgj.bigdata.firstProject
 * @ClassName: MaoLi
 * @Description: TODO
 * @Version 1.0
 */
object MaoLi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job MaoLi.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    spark.stop()
  }
  def runRES(spark: SparkSession): Unit = {
    import spark.implicits._
    //进行列裁剪
//  T_AR_RECEIVABLE
    spark.sql(
      s"""
        |SELECT
        |FCREATEDATE,
        |fid,
        |FCUSTOMERID,
        |Fpayorgid
        |FROM
        | ${TableName.T_AR_RECEIVABLE}
        |WHERE F_ORDERTYPE='2' and FDOCUMENTSTATUS='C'
        |""".stripMargin).createOrReplaceTempView("t_AR_receivable")
    //T_AR_RECEIVABLEENTRY
    spark.sql(
      s"""
        |SELECT
        |FPRICEQTY,
        |fid,
        |FENTRYTAXRATE,
        |FALLAMOUNTFOR,
        |FPRICEQTY,
        |fentryid,
        |FPROJECTNO,
        |Fcontectcust,
        |Fmaterialid
        |FROM
        | ${TableName.T_AR_RECEIVABLEENTRY}
        |""".stripMargin).createOrReplaceTempView("t_AR_receivableEntry")

    //T_AR_SOCCOSTENTRY
    spark.sql(
      s"""
         |SELECT
         |fdentryid,
         |fid,
         |FCOSTAMT
         |FROM
         | ${TableName.T_AR_SOCCOSTENTRY}
         |""".stripMargin).createOrReplaceTempView("T_AR_SOCCOSTENTRY")

spark.sql(
  s"""
    |select
    |fmaterialid,
    |fnumber,
    |FMATERIALGROUP,
    |F_PAEZ_BASE
    |from ${TableName.T_BD_MATERIAL}
    |""".stripMargin).createOrReplaceTempView("t_Bd_Material")

    spark.sql(
      s"""
        |select
        |fmaterialid,
        |Fname,
        |FSPECIFICATION
        |from
        | ${TableName.T_BD_MATERIAL_L}
        |""".stripMargin).createOrReplaceTempView("t_Bd_Material_l")



    spark.sql(
      s"""
        |select
        |t1.fnumber ProNo,
        |t1_L.fname PRONAME,
        |t2_L.Fname CUSTNAME,
        |t3_L.Fname CONECTCUSTNAME,
        |t4_Brand_L.fname BRANDNAME,
        |t4_Brand_Type_L.Fname BRANDTYPE,
        |t4.fnumber MATERIALNO,
        |t4_L.Fname MATERIALNAME,
        |t4_L.FSPECIFICATION TYPE,
        |t4_Group.Fnumber MATERIALGROUP,
        |nvl(b.FPRICEQTY,0) SALEQTY,
        |nvl(b.FENTRYTAXRATE,0)/100 TAX,
        |nvl(b.FALLAMOUNTFOR,0) TAXAMOUNT,
        |case when t1_Z.FNumber=t1_H.FNumber then 0
        |  else nvl(t1.FBEHALFINVOICERATIO,0)/100 end DKRate,
        |a.FCREATEDATE CREATEDATE,
        |nvl(b.FPRICEQTY,0) CostQty,
        |nvl(a_Cost.FCOSTAMT,0) CostAmount,
        |t1.FBEHALFINVOICERATIO DKBL
        |from t_AR_receivable a
        |join t_AR_receivableEntry b on a.fid=b.fid
        |left join T_AR_SOCCOSTENTRY a_Cost on a_Cost.fdentryid=b.fentryid and a_Cost.fid=a.fid
        |join ${TableName.PXDF_PROJECTBASIC} t1 on t1.fid=b.FPROJECTNO
        |join ${TableName.PXDF_PROJECTBASIC_L} t1_L on t1.fid=t1_L.Fid
        |join ${TableName.T_ORG_ORGANIZATIONS} t1_Z on t1_Z.Forgid=t1.F_PAEZ_ORGID
        |join ${TableName.T_ORG_ORGANIZATIONS} t1_H on t1_H.Forgid=t1.FUSEORGID
        |left join ${TableName.T_BD_CUSTOMER} t2 on t2.fcustid=b.Fcontectcust
        |left join ${TableName.T_BD_CUSTOMER_L} t2_L on t2.fcustid=t2_L.Fcustid
        |left join ${TableName.T_BD_CUSTOMER} t3 on t3.fcustid=a.FCUSTOMERID
        |left join ${TableName.T_BD_CUSTOMER_L} t3_L on t3.fcustid=t3_L.Fcustid
        |join t_Bd_Material t4 on t4.fmaterialid=b.Fmaterialid
        |join t_Bd_Material_l t4_L on t4_L.fmaterialid=t4.fmaterialid
        |join ${TableName.T_BD_MATERIALGROUP} t4_Group on t4_Group.Fid=t4.FMATERIALGROUP
        |left join ${TableName.PAEZ_T_CUST_ENTRY100020} t4_Brand on t4_Brand.Fid=t4.F_PAEZ_BASE
        |left join ${TableName.Paez_t_Cust_Entry100020_l} t4_B rand_L on t4_Brand_L.Fid=t4.F_PAEZ_BASE
        |left join ${TableName.PAEZ_t_Cust_Entry100002_L} t4_Brand_Type_L on t4_Brand_Type_L.Fid=t4_Brand.f_Pxdf_Base
        |join ${TableName.T_ORG_ORGANIZATIONS} t6 on t6.Forgid=a.Fpayorgid
        |join ${TableName.T_ORG_ORGANIZATIONS_L} t6_L on t6_L.Forgid=t6.Forgid
        |""".stripMargin).createOrReplaceTempView("result_reace")

    spark.sql(
      """
        |select
        |ProNo,PRONAME,CUSTNAME,CONECTCUSTNAME,BRANDNAME,BRANDTYPE,MATERIALNO,MATERIALNAME,TYPE,MATERIALGROUP,
        |sum(SALEQTY) SALEQTY,TAX SALETAX,
        |case when DKRATE > 0 then sum(TAXAMOUNT)/DKRATE
        | else sum(TAXAMOUNT) end as TAXSALEAMOUNT,
        |DKRATE,CREATEDATE DeliveryTime,1 FTRate,DKBL,sum(CostQty) CostQty,
        |case when DKRATE > 0 then sum(TAXAMOUNT)/(1+nvl(TAX,0))
        |else sum(CostAmount) end as NOTAXCOSTAMOUNT
        |from
        |result_reace
        |group by ProNo,PRONAME,CUSTNAME,CONECTCUSTNAME,BRANDNAME,BRANDTYPE,MATERIALNO,MATERIALNAME
        |,TYPE,MATERIALGROUP,TAX,DKRATE,CREATEDATE,DKBL
        |""".stripMargin).createOrReplaceTempView("result_ball")

    /**
     * 校准各个金额
     * 按照之前沟通的, 先将进项税率与销项税率一致
     */
    spark.sql(
      """
        |select
        |ProNo,DKBL,PRONAME,CUSTNAME,CONECTCUSTNAME,BRANDNAME,BRANDTYPE,MATERIALNO,MATERIALNAME,TYPE,MATERIALGROUP,SaleQty,SALETAX,
        |(TAXSALEAMOUNT/(1+SALETAX))/(if(nvl(SALEQTY,0)==0,1, SALEQTY)) as NOTAXSALEPRICE,
        |nvl(TAXSALEAMOUNT,0)/(if(nvl(SALEQTY,0)== 0,1,SALEQTY)) as TAXSALEPRICE,
        |TAXSALEAMOUNT/(1+SALETAX) NOTAXSALEAMOUNT,
        |TAXSALEAMOUNT,
        |CostQty,
        |SALETAX CostTax,
        |NOTAXCOSTAMOUNT/(if(nvl(COSTQTY,0) == 0,1 ,COSTQTY)) as NOTAXCOSTPRICE,
        |(NOTAXCOSTAMOUNT *(1+SALETAX))/(if(nvl(COSTQTY,0) == 0,1 ,COSTQTY)) as TAXCOSTPRICE,
        |NoTaxCostAmount,
        |NOTAXCOSTAMOUNT *(1+SALETAX) TAXCOSTAMOUNT,
        |(TAXSALEAMOUNT/(1+SALETAX)) - NoTaxCostAmount as NOTAXGROSS,
        |DeliveryTime
        |from
        |result_ball
        |""".stripMargin).createOrReplaceTempView("result")

    spark.sql(
      """
        |INSERT OVERWRITE TABLE ODS_XHGJ_TMP.MAOLI
        |SELECT
        |NVL(ProNo,''),
        |NVL(DKBL,''),
        |NVL(PRONAME,''),
        |NVL(CUSTNAME,''),
        |NVL(CONECTCUSTNAME,''),
        |NVL(BRANDNAME,''),
        |NVL(BRANDTYPE,''),
        |NVL(MATERIALNO,''),
        |NVL(MATERIALNAME,''),
        |NVL(TYPE,''),
        |NVL(MATERIALGROUP,''),
        |SaleQty,
        |CAST(SALETAX AS decimal(6,2)),
        |CAST(NOTAXSALEPRICE AS decimal(23,10)) NOTAXSALEPRICE,
        |CAST(TAXSALEPRICE AS decimal(23,10)) TAXSALEPRICE,
        |CAST(NOTAXSALEAMOUNT AS decimal(23,10)) NOTAXSALEAMOUNT,
        |CAST(TAXSALEAMOUNT AS decimal(23,10)) TAXSALEAMOUNT,
        |CostQty,
        |CAST(CostTax AS decimal(6,2)),
        |CAST(NOTAXCOSTPRICE AS decimal(23,10)) NOTAXCOSTPRICE,
        |CAST(TAXCOSTPRICE AS decimal(23,10)) TAXCOSTPRICE,
        |CAST(NoTaxCostAmount AS decimal(23,10)) NoTaxCostAmount,
        |CAST(TAXCOSTAMOUNT AS decimal(23,10)) TAXCOSTAMOUNT,
        |CAST(NOTAXGROSS AS decimal(23,10)) NOTAXGROSS,
        |NVL(DeliveryTime,'')
        |from
        |result
        |""".stripMargin)


  }

  def showDf(result: DataFrame, num: Int): Unit = {
    println("result num is ===="+result.count())
    result.show(num)
  }
}

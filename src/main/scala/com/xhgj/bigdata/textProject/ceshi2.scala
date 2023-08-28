package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.TableName
import org.apache.spark.sql.SparkSession

/**
 * @Author luoxin
 * @Date 2023/8/23 10:05
 * @PackageName:com.xhgj.bigdata.textProject
 * @ClassName: ceshi2
 * @Description: TODO
 * @Version 1.0
 */
object ceshi2 {
  val takeRow = 20

  case class Params(inputDay: String = null)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark task job ceshi.scala")
      .enableHiveSupport()
      .getOrCreate()

    runRES(spark)
    //关闭SparkSession
    spark.stop()
  }
  def runRES(spark: SparkSession)={
    spark.sql(
      s"""
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME ,SUM(OERE.FRECAMOUNTFOR_E) REAMOUNT
         |FROM ${TableName.ODS_ERP_RECEIVEBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVEBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FPAYUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FPAYORGID = '2297156' AND OER.FDOCUMENTSTATUS = 'C'
         |	AND DC.FNAME not in ('咸亨国际科技股份有限公司','DP咸亨国际科技股份有限公司') AND SUBSTRING(OER.FCREATEDATE,1,10) >= '2023-06-01' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |UNION ALL
         |SELECT DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME,SUM(OERE.FRECAMOUNTFOR_E) REAMOUNT
         |FROM ${TableName.ODS_ERP_RECEIVEBILL} OER
         |LEFT JOIN ${TableName.ODS_ERP_RECEIVEBILLENTRY} OERE ON OER.FID = OERE.FID
         |LEFT JOIN ${TableName.DIM_CUSTOMER} DC ON OER.FPAYUNIT = DC.FCUSTID
         |LEFT JOIN ${TableName.DIM_PROJECTBASIC} DP ON OERE.FPROJECTNO = DP.FID
         |left join ${TableName.ODS_ERP_BIGTICKETPROJECT} big on DP.fnumber= big.fbillno
         |LEFT JOIN ${TableName.DWD_WRITE_PROJECTNAME} DWP ON DP.FNAME = DWP.PROJECTNAME
         |WHERE OER.FDOCUMENTSTATUS = 'C' AND OER.FPAYORGID = '910474' AND big.F_PAEZ_TEXT1 = '咸亨国际电子商务有限公司'
         |	AND DWP.PROJECTSHORTNAME != '中核集团' AND SUBSTRING(OER.FCREATEDATE,1,10) >= '2023-06-01'
         |GROUP BY DP.FNUMBER,DP.FNAME,DWP.PROJECTSHORTNAME
         |""".stripMargin).createOrReplaceTempView("A4")


  }

}

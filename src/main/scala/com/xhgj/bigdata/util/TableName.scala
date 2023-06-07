package com.xhgj.bigdata.util

/**
 * @Author luoxin
 * @Date 2023/4/26 11:08
 * @PackageName:com.xhgj.bigdata.util
 * @ClassName: TableName
 * @Description: TODO
 * @Version 1.0
 */
object TableName {
  val ODS = "ODS_XHGJ."
  val ODS_TMP = "ODS_XHGJ_TMP."
  val DW = "DW_XHGJ."
  //费用类型的列表表名
  val FEE_TYPR_TABLENAEM = ODS + "YKB_FEE_TYPE"
  val T_AR_RECEIVABLEENTRY = ODS_TMP + "T_AR_RECEIVABLEENTRY"
  val T_AR_RECEIVABLE = ODS_TMP + "T_AR_RECEIVABLE"
  val PXDF_PROJECTBASIC = ODS_TMP + "PXDF_PROJECTBASIC"
  val PXDF_PROJECTBASIC_L = ODS_TMP + "PXDF_PROJECTBASIC_L"
  val T_ORG_ORGANIZATIONS = ODS_TMP + "T_ORG_ORGANIZATIONS"
  val T_BD_CUSTOMER = ODS_TMP + "T_BD_CUSTOMER"
  val T_BD_CUSTOMER_L = ODS_TMP + "T_BD_CUSTOMER_L"
  val T_ORG_ORGANIZATIONS_L = ODS_TMP + "T_ORG_ORGANIZATIONS_L"
  val T_BD_MATERIAL = ODS_TMP + "T_BD_MATERIAL"
  val T_BD_MATERIAL_L = ODS_TMP + "T_BD_MATERIAL_L"
  val T_BD_MATERIALGROUP = ODS_TMP + "T_BD_MATERIALGROUP"
  val PXDF_WRITEOFFADJUST = ODS_TMP + "PXDF_WRITEOFFADJUST"
  val T_AR_SOCCOSTENTRY = ODS_TMP + "T_AR_SOCCOSTENTRY"
  val T_AR_RECEIVABLEENTRY_O = ODS_TMP + "T_AR_RECEIVABLEENTRY_O"
  val PAEZ_T_CUST_ENTRY100020 = ODS_TMP + "PAEZ_T_CUST_ENTRY100020"
  val Paez_t_Cust_Entry100020_l = ODS_TMP + "Paez_t_Cust_Entry100020_l"
  val PAEZ_t_Cust_Entry100002_L = ODS_TMP + "PAEZ_t_Cust_Entry100002_L"


  //ODS层所用到的增量表
  val ODS_ERP_RECEIVABLE=ODS+"ODS_ERP_RECEIVABLE"
  val ODS_ERP_RECEIVABLEENTRY=ODS+"ODS_ERP_RECEIVABLEENTRY"
  val ODS_ERP_SALORDER=ODS+"ODS_ERP_SALORDER"
  val ODS_ERP_SALORDERENTRY=ODS+"ODS_ERP_SALORDERENTRY"
  val ODS_ERP_POORDER=ODS+"ODS_ERP_POORDER"
  val ODS_ERP_POORDERENTRY=ODS+"ODS_ERP_POORDERENTRY"
  val ODS_ERP_INSTOCK=ODS+"ODS_ERP_INSTOCK"
  val ODS_ERP_INSTOCKENTRY=ODS+"ODS_ERP_INSTOCKENTRY"
  val ODS_ERP_INSTOCKENTRY_F=ODS+"ODS_ERP_INSTOCKENTRY_F"
  val ODS_ERP_DELIVERYNOTICE=ODS+"ODS_ERP_DELIVERYNOTICE"
  val ODS_ERP_DELIVERYNOTICEENTRY=ODS+"ODS_ERP_DELIVERYNOTICEENTRY"
  val ODS_ERP_OUTSTOCK=ODS+"ODS_ERP_OUTSTOCK"
  val ODS_ERP_OUTSTOCKENTRY=ODS+"ODS_ERP_OUTSTOCKENTRY"
  val ODS_ERP_OUTSTOCKENTRY_F=ODS+"ODS_ERP_OUTSTOCKENTRY_F"
  val ODS_ERP_MATERIAL=ODS+"ODS_ERP_MATERIAL"
  //ODS层其他表
  val ODS_ERP_SALORDERENTRY_F=ODS+"ODS_ERP_SALORDERENTRY_F"
  val ODS_ERP_BIGTICKETPROJECT=ODS+"ODS_ERP_BIGTICKETPROJECT"
  //ODS层维度表
  val ODS_ERP_CUSTOMER = ODS + "ODS_ERP_CUSTOMER"
  val ODS_ERP_CUSTOMER_L = ODS + "ODS_ERP_CUSTOMER_L"
  val ODS_ERP_USER = ODS + "ODS_ERP_USER"
  val ODS_ERP_USER_L = ODS + "ODS_ERP_USER_L"
  //DWD层

  val DIM_CUSTOMER=DW+"DIM_CUSTOMER"
  val DIM_USER=DW+"DIM_USER"
  val DIM_SALEMAN=DW+"DIM_SALEMAN"
  val DIM_OPERATOR=DW+"DIM_OPERATOR"
  val DIM_MATERIAL=DW+"DIM_MATERIAL"
  val DIM_PAEZ_ENTRY100020=DW+"DIM_PAEZ_ENTRY100020"
  val DIM_UNIT=DW+"DIM_UNIT"
  val DIM_PROJECTBASIC=DW+"DIM_PROJECTBASIC"
  val DWD_SALE_ORDERENTRY=DW+"DWD_SALE_ORDERENTRY"
  val DWD_SALE_ORDER = DW+"DWD_SALE_ORDER"
  val DWD_SALE_BIGTICKETPROJECT = DW+"DWD_SALE_BIGTICKETPROJECT"


}

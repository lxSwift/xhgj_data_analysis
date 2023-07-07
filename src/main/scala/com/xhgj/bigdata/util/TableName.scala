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
  val ODS_ERP_MATERIALPURCHASE=ODS+"ODS_ERP_MATERIALPURCHASE"
  val ODS_ERP_SALORDER=ODS+"ODS_ERP_SALORDER"
  val ODS_ERP_SALORDERENTRY=ODS+"ODS_ERP_SALORDERENTRY"
  val ODS_ERP_POORDER=ODS+"ODS_ERP_POORDER"
  val ODS_ERP_PAYBILL=ODS+"ODS_ERP_PAYBILL"
  val ODS_ERP_PAYBILLENTRY=ODS+"ODS_ERP_PAYBILLENTRY"
  val ODS_ERP_POORDERENTRY=ODS+"ODS_ERP_POORDERENTRY"
  val ODS_ERP_POORDERENTRY_F=ODS+"ODS_ERP_POORDERENTRY_F"
  val ODS_ERP_POORDERENTRY_R=ODS+"ODS_ERP_POORDERENTRY_R"
  val ODS_ERP_POORDERENTRY_LK=ODS+"ODS_ERP_POORDERENTRY_LK"
  val ODS_ERP_INSTOCK=ODS+"ODS_ERP_INSTOCK"
  val ODS_ERP_RECEIVEBILL=ODS+"ODS_ERP_RECEIVEBILL"
  val ODS_ERP_RECEIVEBILLENTRY=ODS+"ODS_ERP_RECEIVEBILLENTRY"
  val ODS_ERP_REFUNDBILL=ODS+"ODS_ERP_REFUNDBILL"
  val ODS_ERP_REFUNDBILLENTRY=ODS+"ODS_ERP_REFUNDBILLENTRY"
  val ODS_ERP_INSTOCKENTRY=ODS+"ODS_ERP_INSTOCKENTRY"
  val ODS_ERP_INSTOCKENTRY_LK=ODS+"ODS_ERP_INSTOCKENTRY_LK"
  val ODS_ERP_INSTOCKENTRY_F=ODS+"ODS_ERP_INSTOCKENTRY_F"
  val ODS_ERP_DELIVERYNOTICE=ODS+"ODS_ERP_DELIVERYNOTICE"
  val ODS_ERP_DELIVERYNOTICEENTRY=ODS+"ODS_ERP_DELIVERYNOTICEENTRY"
  val ODS_ERP_OUTSTOCK=ODS+"ODS_ERP_OUTSTOCK"
  val ODS_ERP_OUTSTOCKENTRY=ODS+"ODS_ERP_OUTSTOCKENTRY"
  val ODS_ERP_OUTSTOCKENTRY_LK=ODS+"ODS_ERP_OUTSTOCKENTRY_LK"
  val ODS_ERP_OUTSTOCKENTRY_F=ODS+"ODS_ERP_OUTSTOCKENTRY_F"
  val ODS_ERP_SALORDERENTRY_E=ODS+"ODS_ERP_SALORDERENTRY_E"
  val ODS_ERP_OUTSTOCKENTRY_R=ODS+"ODS_ERP_OUTSTOCKENTRY_R"
  val ODS_ERP_ARSETTLEMENT=ODS+"ODS_ERP_ARSETTLEMENT"
  val ODS_ERP_ARSETTLEMENTDETAIL=ODS+"ODS_ERP_ARSETTLEMENTDETAIL"
  val ODS_ERP_MATERIAL=ODS+"ODS_ERP_MATERIAL"
  val ODS_ERP_PAYABLE = ODS+"ODS_ERP_PAYABLE"
  val ODS_ERP_PAYABLEENTRY = ODS+"ODS_ERP_PAYABLEENTRY"
  val ODS_ERP_PAYABLEENTRY_O = ODS+"ODS_ERP_PAYABLEENTRY_O"
  val ODS_ERP_REQUISITION = ODS+"ODS_ERP_REQUISITION"
  val ODS_ERP_REQENTRY = ODS+"ODS_ERP_REQENTRY"
  val ODS_ERP_REQENTRY_S = ODS+"ODS_ERP_REQENTRY_S"
  val ODS_ERP_REQENTRY_R = ODS+"ODS_ERP_REQENTRY_R"
  val ODS_ERP_REQENTRY_LK = ODS+"ODS_ERP_REQENTRY_LK"
  val ODS_EAS_ACCOUNTBALANCE = ODS+"ODS_EAS_ACCOUNTBALANCE"
  val ODS_ERP_RETURNSTOCK = ODS+"ODS_ERP_RETURNSTOCK"
  val ODS_ERP_RETURNSTOCKENTRY = ODS+"ODS_ERP_RETURNSTOCKENTRY"
  //ODS层其他表
  val ODS_ERP_SALORDERENTRY_F=ODS+"ODS_ERP_SALORDERENTRY_F"
  val ODS_OA_STAFF=ODS+"ODS_OA_STAFF"
  val ODS_OA_ORG_ELEMENT=ODS+"ODS_OA_ORG_ELEMENT"
  val ODS_OA_ORG_PERSON=ODS+"ODS_OA_ORG_PERSON"
  val ODS_ERP_BIGTICKETPROJECT=ODS+"ODS_ERP_BIGTICKETPROJECT"
  //ODS层维度表
  val ODS_ERP_CUSTOMER = ODS + "ODS_ERP_CUSTOMER"
  val ODS_ERP_CUSTOMER_L = ODS + "ODS_ERP_CUSTOMER_L"
  val ODS_ERP_USER = ODS + "ODS_ERP_USER"
  val ODS_ERP_USER_L = ODS + "ODS_ERP_USER_L"
  //DWD层

  val DIM_CUSTOMER=DW+"DIM_CUSTOMER"
  val DIM_BILLTYPE=DW+"DIM_BILLTYPE"
  val DIM_CUSTOMERMANAGE=DW+"DIM_CUSTOMERMANAGE"
  val DWD_WRITE_PROJECTNAME=DW+"DWD_WRITE_PROJECTNAME"
  val DWD_HISTORY_RECEIVABLE=DW+"DWD_HISTORY_RECEIVABLE"
  val DWD_WRITE_COMPANYNAME=DW+"DWD_WRITE_COMPANYNAME"
  val DIM_USER=DW+"DIM_USER"
  val DIM_SALEMAN=DW+"DIM_SALEMAN"
  val DIM_OPERATOR=DW+"DIM_OPERATOR"
  val DIM_MATERIAL=DW+"DIM_MATERIAL"
  val DIM_LOTMASTER=DW+"DIM_LOTMASTER"
  val DIM_INVBAL=DW+"DIM_INVBAL"
  val DIM_ORGANIZATIONS=DW+"DIM_ORGANIZATIONS"
  val DIM_DEPARTMENT=DW+"DIM_DEPARTMENT"
  val DIM_STAFF=DW+"DIM_STAFF"
  val DIM_BUYER=DW+"DIM_BUYER"
  val DIM_SUPPLIER=DW+"DIM_SUPPLIER"
  val DIM_SUPPLIERCONTACT=DW+"DIM_SUPPLIERCONTACT"
  val DIM_CURRENCY_ERP=DW+"DIM_CURRENCY_ERP"
  val DIM_SETTLETYPE=DW+"DIM_SETTLETYPE"
  val DIM_PAEZ_ENTRY100020=DW+"DIM_PAEZ_ENTRY100020"
  val DIM_CUST100501=DW+"DIM_CUST100501"
  val DIM_STOCK=DW+"DIM_STOCK"
  val DIM_OASTAFF=DW+"DIM_OASTAFF"
  val DIM_UNIT=DW+"DIM_UNIT"
  val DIM_PROJECTBASIC=DW+"DIM_PROJECTBASIC"
  val DIM_ENTRY100504=DW+"DIM_ENTRY100504"
  val DIM_FLEXVALUESENTRY=DW+"DIM_FLEXVALUESENTRY"
  val DIM_EMPINFO=DW+"DIM_EMPINFO"
  val DWD_SALE_ORDERENTRY=DW+"DWD_SALE_ORDERENTRY"
  val DWD_SALE_ORDER = DW+"DWD_SALE_ORDER"
  val DWD_SALE_BIGTICKETPROJECT = DW+"DWD_SALE_BIGTICKETPROJECT"

  val DWS_RECE_PAYAMOUNT = DW+"DWS_RECE_PAYAMOUNT"
  val DWS_PUR_DATEINFO = DW+"DWS_PUR_DATEINFO"
  val DWS_RECE_PAYAMOUNTGET = DW+"DWS_RECE_PAYAMOUNTGET"
  val DWD_PUR_REQUISITION = DW+"DWD_PUR_REQUISITION"
  val DWD_PUR_POORDER = DW+"DWD_PUR_POORDER"
  val DWD_PUR_INSTOCK = DW+"DWD_PUR_INSTOCK"
  val DWD_SAL_DELIVERYNOTICE = DW+"DWD_SAL_DELIVERYNOTICE"
  val DWD_SAL_OUTSTOCK = DW+"DWD_SAL_OUTSTOCK"
  val DWD_FIN_ARSETTLEMENT = DW+"DWD_FIN_ARSETTLEMENT"
  val DWD_SAL_ORDER = DW+"DWD_SAL_ORDER"
  val DIM_COMPANY = DW+"DIM_COMPANY"
  val DIM_ACCOUNTVIEW = DW+"DIM_ACCOUNTVIEW"
  val DIM_CURRENCY = DW+"DIM_CURRENCY"

}

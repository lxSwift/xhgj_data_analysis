package com.xhgj.bigdata.util

import scala.io.Source
import scala.util.parsing.json.JSON
/**
 * @Author luoxin
 * @Date 2023/10/24 9:52
 * @PackageName:com.xhgj.bigdata.util
 * @ClassName: ApiData
 * @Description: 根据url来获取json数据
 * @Version 1.0
 */
object ApiData {
  def main(args: Array[String]): Unit = {
    val apiUrl = "https://dd2.ekuaibao.com/api/openapi/v1/feeTypes?accessToken=ID01tY88ea3hTN%3AID_3yr82_w5sUg"

    // 发起 HTTP 请求获取 JSON 数据
    val jsonString = Source.fromURL(apiUrl).mkString
    // 解析 JSON 数据
    val parsedJson = JSON.parseFull(jsonString)

    parsedJson match {
      case Some(json: Map[String, Any]) =>
        // 在这里处理解析得到的 JSON 数据
        json.get("items") match {
          case Some(dataList: List[Any]) if dataList.nonEmpty =>
            val firstElement: Any = dataList.head
            println(firstElement)
          case _ =>
            println("Empty data list or invalid JSON structure")
        }
          case None =>
        println("Failed to parse JSON")
    }
  }

}

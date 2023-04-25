package com.xhgj.bigdata.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.collections.iterators.ArrayListIterator

import scala.collection.mutable.ArrayBuffer
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.immutable
import scala.io.Source

/**
 * 解析复杂的json数据串
 * 工具: fastjson
 * 目的: 解析易快报单据的json数据,将需要的字段数据拉取出来
 * 想法: 使用spark进行转化成df将数据导入至hive表中
 */
object JsonParse {

  def main(args: Array[String]): Unit = {
    val lines: String = Source.fromFile("D:\\response2.json").mkString
    amount(lines)
  }

/**
 *@Description:获得JSON请求体 form - > details - > feeTypeForm - > amount里面的数据方法
 *@Param: [jsonstr]
 *@Return: scala.Tuple3<java.lang.String,java.lang.String,java.lang.String>[]
 *@DateTime: 13:57 2023/4/25
 */
  def amount(jsonstr:String) = {
    //将{}的字符串解析成json
    val jsonOBJ: JSONObject = JSON.parseObject(jsonstr)
    //获取items下面的jsonobj数组(示例中有十个单位)
    val itemsJsonOBJArray: JSONArray = jsonOBJ.getJSONArray("items")
    val len = itemsJsonOBJArray.length
    val arrb = new ArrayBuffer[(String,String,String)]()
    //获取第一个jsnobj(后面改成for进行提取)
    for (i <- 0 until len ){
      val firstjsonOBJ = itemsJsonOBJArray.getJSONObject(i)
      //获取form结果字符串
      val obj1: String = firstjsonOBJ.getString("form")
      val formjsonOBJ = JSON.parseObject(obj1)
      val titile = formjsonOBJ.getString("title")
      val detailsOBJArray = formjsonOBJ.getJSONArray("details")
      val ffjsonOBJ = detailsOBJArray.getJSONObject(0)
      val feeTypeFormStr = ffjsonOBJ.getString("feeTypeForm")
      val feeTypeFormOBJ = JSON.parseObject(feeTypeFormStr)
      val amountStr = feeTypeFormOBJ.getString("amount")
      val amountOBJ = JSON.parseObject(amountStr)
      val standard = amountOBJ.getString("standard")
      val standardUnit = amountOBJ.getString("standardUnit")
      arrb.append((titile,standard,standardUnit))
      println("titile=" + arrb(i))
    }
     arrb.toArray
  }
}

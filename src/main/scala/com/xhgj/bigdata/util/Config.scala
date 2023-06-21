package com.xhgj.bigdata.util

import java.io.{BufferedInputStream, File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * 用于获取配置文件的内容
 */
object Config {
//  val configPath = System.getProperty("configPath",Thread.currentThread().getContextClassLoader.getResource(".").getPath)
//  println("config director:" + configPath)
   def load(propertiesName:String)={
//     val prop = new Properties()
//
//     prop.load(new InputStreamReader(
//       new FileInputStream(configPath+File.separator+propertiesName),StandardCharsets.UTF_8
//     ))
//     println("config filepath:" + configPath+File.separator+propertiesName)
//     prop
//     val directory = new File("..")
//     val filePath = directory.getAbsolutePath
//     val postgprop = new Properties
//     val ipstream = new BufferedInputStream(new FileInputStream("/home/project/mytest/conf/"+propertiesName))
//     postgprop.load(ipstream)
//     postgprop
// 指定配置文件路径
     val filePath = "/home/project/xhgj_firstpro/conf/"+propertiesName
     // 创建文件输入流
     val fis = new FileInputStream(filePath)
     // 创建 Properties 对象并加载配置文件
     val properties = new Properties()
     properties.load(fis)
     // 关闭流
     fis.close()
     properties
   }

  def loadWind(propertiesName:String)={
    //读取本地路径下的配置文件
    val directory = new File("./src/main/resources")
    val filePath = directory.getAbsolutePath
    println(filePath)
    val postgprop = new Properties
    val ipstream = new BufferedInputStream(new FileInputStream(filePath+"/"+propertiesName))
    postgprop.load(ipstream)
    ipstream.close()
    postgprop

  }
  def main(args: Array[String]): Unit = {
    val prop =Config.loadWind("config.properties")
    val pro = prop.getProperty("table_incr")
    //把pro参数按照逗号切割成列表
    val listable = pro.split(",")
    for(i <- listable){
      val j = i.split("--")
      println(i)
      println(j(0))
      println(j(1))
      println(j(2))

    }
  }
}

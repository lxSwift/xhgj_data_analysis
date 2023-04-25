package com.xhgj.bigdata.util

import java.io.{BufferedInputStream, File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * 用于获取配置文件的内容
 */
object Config {
  val configPath = System.getProperty("configPath",Thread.currentThread().getContextClassLoader.getResource(".").getPath)
  println("config director:" + configPath)
   def load(propertiesName:String):Properties={
//     val prop = new Properties()
//
//     prop.load(new InputStreamReader(
//       new FileInputStream(configPath+File.separator+propertiesName),StandardCharsets.UTF_8
//     ))
//     println("config filepath:" + configPath+File.separator+propertiesName)
//     prop
     val directory = new File("..")
     val filePath = directory.getAbsolutePath
     val postgprop = new Properties
     val ipstream = new BufferedInputStream(new FileInputStream("/home/project/mytest/conf/"+propertiesName))
     postgprop.load(ipstream)
     postgprop
   }

  def main(args: Array[String]): Unit = {
    val prop:Properties =Config.load("my.properties")
    val value = prop.getProperty("name")
    println(value)
  }
}

package com.xhgj.bigdata.util
import java.io.{FileInputStream, IOException}
import java.util.Properties
/**
 * @Author luoxin
 * @Date 2023/6/8 13:27
 * @PackageName:com.xhgj.bigdata.util
 * @ClassName: ReadProperties
 * @Description: TODO
 * @Version 1.0
 */
object ReadProperties {
  def main(args: Array[String]): Unit = {

    // 指定配置文件路径
    val filePath = "/path/to/config.properties"

    // 创建文件输入流
    val fis = new FileInputStream(filePath)

    // 创建 Properties 对象并加载配置文件
    val properties = new Properties()
    properties.load(fis)

    // 读取配置文件中的属性和值
    val url = properties.getProperty("database.url")
    val user = properties.getProperty("database.user")
    val password = properties.getProperty("database.password")

    // 输出结果
    println("url: " + url)
    println("user: " + user)
    println("password: " + password)

    // 关闭流
    fis.close()
  }
}

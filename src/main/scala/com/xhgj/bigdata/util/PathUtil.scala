package com.xhgj.bigdata.util
import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.spark.SparkContext
object PathUtil {
  /**
   * 判断hdfs路径是否存在
   */
  def isExisted(sc: SparkContext, path: String): Boolean = {
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    hdfs.exists(new Path(path)) match {
      case true =>
        println("this output path is exited:" + hdfs.makeQualified(new Path(path))); true
      case _ => false
    }
  }

  /**
   * 删除已存在的文件
   */
  def deleteExistedPath(sc:SparkContext,path:String)={
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(path),true)
    } catch {case e:Throwable => throw(e)}
  }
}

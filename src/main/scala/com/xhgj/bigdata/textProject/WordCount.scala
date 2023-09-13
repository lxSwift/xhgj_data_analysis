package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.sql.Types.{VARCHAR,INTEGER,DATE}
import java.util.Properties

/**
 * double bow
 *
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //配置不要把mysql字段注释清空
    val wordPerMessage = 6
    var i = 0
    while (i < 1) {
      /*
      1.the (1 to 1) is meaning that only have one circulation.
      */
      (1 to 1).foreach { messageNum => {
        //[There's only three cycle]
        val str: Seq[Int] = (1 to wordPerMessage).map(x => scala.util.Random.nextInt(32)+1 )
        val str2=(scala.util.Random.nextInt(15)+1).toString

        val str3 = str.appended(str2)
        val str1 = str.mkString(" ") //separate str1 with space
        println(str3)
      }
      }
      i = i + 1
    }
  }

}

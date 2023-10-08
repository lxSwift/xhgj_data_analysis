package com.xhgj.bigdata.textProject

import com.xhgj.bigdata.util.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Types.{DATE, INTEGER, VARCHAR}
import java.util.Properties
import scala.collection.immutable.Set
import scala.util.Random

/**
 * double bow
 *
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //配置不要把mysql字段注释清空
    val wordPerMessage = 6
    var i = 0

    val wordPerMessage2 = 5
    while (i < 1) {
      /*
      1.the (1 to 1) is meaning that only have one circulation.
      */
      (1 to 1).foreach { messageNum => {
        //[There's only three cycle]
        val str4: Seq[Int] = (1 to wordPerMessage2).map(x => scala.util.Random.nextInt(35) + 1)
        val str5 = (1 to 2).map(x =>scala.util.Random.nextInt(12) + 1).toString()

        val str6 = str4.appended(str5)
        val str7 = str4.mkString(" ") //separate str1 with space
        println(str6)
      }
      }
      i = i + 1
    }

    val randomNumbers = getRandomNumbers()
    println(randomNumbers)
  }

  def getRandomNumbers() = {
    val random = new Random()
    val numbers = collection.mutable.Set[Int]()
    while (numbers.size < 5) {
      val num = random.nextInt(35) + 1
      numbers.add(num)
    }
    val numbers2 = collection.mutable.Set[Int]()
    while (numbers2.size < 2) {
      val num = random.nextInt(12) + 1
      numbers2.add(num)
    }
    numbers.mkString(",")+"    "+numbers2.mkString(",")

  }
}

package scala.com.xhgj.bigdata.textProject

import org.apache.spark.{SparkConf, SparkContext}
object WordCount {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("wordCount")
      val sc = new SparkContext(conf)

      val fileRdd = sc.textFile("/spark/sparkhistory/text.txt")

      val wordsRdd = fileRdd.flatMap(line => line.split(" "))

      val wordMapRdd = wordsRdd.map(word => (word, 1))

      val countRdd = wordMapRdd.reduceByKey(_ + _)

      countRdd.collect().foreach(println)

  }
}

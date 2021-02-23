package cn.itcast.core

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的基本操作
  */
object RDDDemo02_Basic {
  def main(args: Array[String]): Unit = {
      //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
      //TODO 1. source/ 加载数据/ 创建RD
    val  line: RDD[String] = sc.textFile("data/input/words.txt") //2

    //TODO 2. transformation
    val result = line.filter(StringUtils.isNotBlank(_))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)


   //TODO 3. sink/输出/action
    result.foreach(println)
    result.saveAsTextFile("data/output/result1")



  }
}

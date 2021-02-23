package cn.itcast.core

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的外部数据源
  */
object RDDDemo10_DataSource  {
  def main(args: Array[String]): Unit = {
    //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //TODO 1. source/ 加载数据/ 创建RDD
    //  RDD 一行行数据
    val lines: RDD[String] = sc.textFile("data/input/words.txt")

    //TODO 2. transformation
    val result = lines.filter(StringUtils.isNotBlank(_))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    //TODO 3. sink/输出/action
    // 输出到文本文件
    result.repartition(1).saveAsTextFile("data/output/result1")
    // 输出到对象文件
    result.repartition(1).saveAsObjectFile("data/output/result2")
    // 输出到Sequence文件 比如图片
    result.repartition(1).saveAsSequenceFile("data/output/result3")
  }
}

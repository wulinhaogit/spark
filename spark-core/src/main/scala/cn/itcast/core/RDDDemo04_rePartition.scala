package cn.itcast.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的重分区函数 操作
  */
object RDDDemo04_rePartition {
  def main(args: Array[String]): Unit = {
      //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
      //TODO 1. source/ 加载数据/ 创建RDD
      val rdd1: RDD[Int] = sc.parallelize(1 to 10)  //8
      // repartition 可以增加或者减少分区数, 注意原来的不变
      val rdd2 = rdd1.repartition(9) // 底层是 coalesce (分区数,shuffle =true)
      val rdd3 = rdd1.repartition(7)
      println(rdd1.getNumPartitions) //8
      println(rdd2.getNumPartitions)  //9
      println(rdd3.getNumPartitions)  //7
      // coalesce 默认只能减少分区数,除非把shuffle指定为 false ,注意原来的不变
      val rdd4 = rdd1.coalesce(9)  // 底层是 coalesce (分区数,shuffle =false)
      val rdd5 = rdd1.coalesce(7)
      val rdd6 = rdd1.coalesce(9,true)
      println(rdd4.getNumPartitions) //8
      println(rdd5.getNumPartitions)  //7
      println(rdd6.getNumPartitions)  //9
  }
}

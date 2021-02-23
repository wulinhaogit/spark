package cn.itcast.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的创建
  */
object RDDDemo01_Create {
  def main(args: Array[String]): Unit = {
      //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
      //TODO 1. source/ 加载数据/ 创建RDD
    //parallelize  创建本地集合 ,分区数
      val rdd1: RDD[Int] = sc.parallelize(1 to 10)  //8
      val rdd2: RDD[Int] = sc.parallelize(1 to 10,3)// 3
      val rdd3: RDD[Int] = sc.makeRDD(1 to 10)  //底层是 parallelize 8
      val rdd4: RDD[Int] = sc.parallelize(1 to 10,4)  //4
    //创建  本地/HDFS文件/文件夹/,分区数   注意不要用它来读取大量的小文件
    val  rdd5: RDD[String] = sc.textFile("data/input/words.txt") //2
    val  rdd6: RDD[String] = sc.textFile("data/input/words.txt",4) //4

    // 不要使用 textFile 读取目录  因为里面有多少个小文件 就会有多少个分区 浪费资源
    val  rdd7: RDD[String] = sc.textFile("data/output/result") //2
    val  rdd8: RDD[String] = sc.textFile("data/output/result",4) //5

    //  创建  本地/HDFS文件/文件夹/,分区数  专门用来读取小文件的
    val rdd9: RDD[(String,String)] = sc.wholeTextFiles("data/output/result") //1
    val rdd10: RDD[(String,String)] = sc.wholeTextFiles("data/output/result",4) //1

    println(rdd1.getNumPartitions) // 8    底层partitions.length  获取RDD的分区数
    println(rdd1.partitions.length) //8
    println(rdd2.getNumPartitions) //3
    println(rdd3.getNumPartitions) //8
    println(rdd4.getNumPartitions) //4
    println(rdd5.getNumPartitions) //2
    println(rdd6.getNumPartitions) //4
    println(rdd7.getNumPartitions) //2
    println(rdd8.getNumPartitions) //5
    println(rdd9.getNumPartitions) //1
    println(rdd10.getNumPartitions) //1
    //TODO 2. transformation
      //TODO 3. sink/输出

  }
}

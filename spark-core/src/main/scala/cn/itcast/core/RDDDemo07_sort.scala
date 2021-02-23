package cn.itcast.core

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的 排序
  */
object RDDDemo07_sort {
  def main(args: Array[String]): Unit = {
    //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //TODO 1. source/ 加载数据/ 创建RDD
    //  RDD 一行行数据
    val lines: RDD[String] = sc.textFile("data/input/words.txt")

    //TODO 2. transformation
    val result = lines.filter(StringUtils.isNotBlank(_)).
      flatMap(_.split(" ")).
      map((_,1)).
      reduceByKey(_+_)
   // sortBy _._2 去 _的第二个 这里是value
    val sortResult1 = result.sortBy(_._2, false) //ascending  =false 降序
      .take(3)//取前三个
    //sortByKey   result.map(_.swap) 现将 key value 交换
    val sortResult2 = result.map(_.swap).sortByKey(false).take(3)

    //top 取前3  结果数很小的时候使用   result.map(_.swap) 现将 key value 交换
    val sortResult3 = result.top(3)(Ordering.by(_._2))  //top 默认是降序
    //TODO 3. sink/输出/action
    result.foreach(println)

    println(s"---------------------sortBy-------------------------------")
    sortResult1.foreach(println)

    println(s"-------------------------sortByKey---------------------------")
    sortResult2.foreach(println)

    println(s"--------------------top--------------------------------")
    sortResult3.foreach(println)
  }
}

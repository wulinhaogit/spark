package cn.itcast.core

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的聚合算子   有key的
  *
  */
object RDDDemo05_Aggregate_Key {
  def main(args: Array[String]): Unit = {
      //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
      //TODO 1. source/ 加载数据/ 创建RDD
      val lines: RDD[String] = sc.textFile("data/input/words.txt")
      val wordAndOneRDD: RDD[(String,Int)] = lines.filter(StringUtils.isNotBlank(_))
          .flatMap(_.split(" "))
          .map((_,1))
    // 分组+聚合
    val grouped = wordAndOneRDD.groupByKey()
    val result: RDD[(String,Int)] = grouped.mapValues(_.sum)
    // reduceByKey
    val result2: RDD[(String,Int)] = wordAndOneRDD.reduceByKey(_+_)
    // foldByKey(初始值)(全局聚合)
    val result3: RDD[(String,Int)] = wordAndOneRDD.foldByKey(0)(_+_)
    //aggregateByKey (初始值)(局部聚合,全局聚合)
    val result4: RDD[(String,Int)] = wordAndOneRDD.aggregateByKey(0)(_+_,_+_)

    //TODO 2. transformation
    //TODO 3. sink/输出/action
    // 需求 rdd1中各各元素的和
   result.foreach(println)
   result2.foreach(println)
   result3.foreach(println)
   result4.foreach(println)
  }
}

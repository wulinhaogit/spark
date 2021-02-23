package cn.itcast.core

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的分区操作
  */
object RDDDemo03_PartitionOperation {
  def main(args: Array[String]): Unit = {
      //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
      //TODO 1. source/ 加载数据/ 创建RD
    val  line: RDD[String] = sc.textFile("data/input/words.txt") //2

    //TODO 2. transformation
    val result = line.filter(StringUtils.isNotBlank(_))
      .flatMap(_.split(" "))
     // .map((_, 1))
     /* .map( word => {
      // 开启连接--有几条数据就执行几次
        (word,1)
      // 关闭连接--有几条数据就执行几次
      })*/
      // mapPartitions[Iterator]  迭代器
      .mapPartitions(iter =>{ // 注意mapPartitions 针对每个分区进行的操作
          // 开启连接--有几个分区就执行几次
          iter.map((_,1))  //这里是 迭代器的map  作用在迭代器里的每一条数据上
          // 关闭连接--有几个分区就执行几次
       })
      .reduceByKey(_ + _)


   //TODO 3. sink/输出/action
  //  result.foreach(println)
   /* result.foreach( i =>{
      // 开启连接--有几条数据就执行几次
       println(i)
      // 关闭连接--有几条数据就执行几次
    })*/

    // foreachPartition[Iterator]  迭代器
    result.foreachPartition( iter =>{
      // 开启连接--有几个分区就执行几次
      iter.foreach(println)  //这里是 foreach  作用在迭代器里的每一条数据上
      // 关闭连接--有几个分区就执行几次
    } )
  }
}

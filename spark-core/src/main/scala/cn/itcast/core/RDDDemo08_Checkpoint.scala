package cn.itcast.core

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的  Checkpoint 的演示
  */
object RDDDemo08_Checkpoint {
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

   // TODO ===== 注意:resultRDD 在后续会被频繁使用到,且该RDD 的计算过程比较复杂,所以为了提高访问该RDD的效率,应将该RDD放入缓存中
    //放入缓存
   /* result.cache()  //底层是 persist
    result.persist()  // persist(StorageLevel.MEMORY_ONLY)  存储级别  仅内存
    result.persist(StorageLevel.MEMORY_AND_DISK) //  内存和磁盘  ,内存存不下了 存磁盘*/
    //TODO 上面的持久化/缓存并不能保证RDD数据的绝对安全,所以应使用Checkpoint吧把数据发在DHFS上
    sc.setCheckpointDir("./ckp")//设置Checkpoint的目录 实际中写HDFS的目录
    result.checkpoint() // RDD 进行checkpoint

    // sortBy _._2 去 _的第二个 这里是value
    val sortResult1 = result.sortBy(_._2, false) //ascending  =false 降序
      .take(3)//取前三个
    //sortByKey   result.map(_.swap) 现将 key value 交换
    val sortResult2 = result.map(_.swap).sortByKey(false).take(3)

    //top 取前3  结果数很小的时候使用   result.map(_.swap) 现将 key value 交换
    val sortResult3 = result.top(3)(Ordering.by(_._2))  //top 默认是降序

    // 用完 释放缓存
   // result.unpersist()  //清空缓存
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

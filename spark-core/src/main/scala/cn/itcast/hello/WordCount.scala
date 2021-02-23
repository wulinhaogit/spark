package cn.itcast.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示spark 入门案例 WordCount
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.1")
   //TODO 1.准备sc/sparkContext/Spark上下文环境
    // 1. TODO  建立和Spark框架的连接

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //Spark的核心对象 Spark上下文
    val sc:SparkContext = new SparkContext(sparkConf)
    // 设置日志级别
    sc.setLogLevel("WARN")
    // 2. TODO  执行业务操作
    // 2.1 读取文件 获取一行一行的数据  hello world
    val lines =sc.textFile("data/input/words.txt")
    // 2.2 将一行一行的数据进行拆分成一个一个的单词  hello world =>  hello,world, hello,world
    // 扁平化: jiang
    val words = lines.flatMap(_.split(" "))
    // 2.3 记为1 :RDD[(单词,1)]
    val wordAndOnes: RDD[(String,Int)] = words.map((_,1))
    // 2.4 分组聚合: groupBy+mapValues(_.map(_._2).reduce(_+_))  ===> 在Spark里分组+聚合一步搞定: reduceByKey
    val result = wordAndOnes.reduceByKey(_+_)
    // 3. TODO sink输出
    // 3.1直接输出
    result.foreach(println)
   //3.2 收集为本地集合再输出
    println(result.collect().toBuffer)
    // 3.3 输出到指定path(可以是文件:1/夹:2)
    result.repartition(1).saveAsTextFile("data/output/result")
    // 为了便于查看 web-ui 让程序睡一会
    Thread.sleep(1000*60)
    // 4. TODO  关闭连接
    sc.stop()

  }
}

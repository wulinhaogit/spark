package cn.itcast.core

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的共享变量
  * 需求: 以词频统计WordCount程序为例, 处理的数据如  data/input/words2.txt 所示,
  * 包括非单词符号,统计数据词频时过滤非单词的特殊符号 并且特殊符号的总数
  *
  * 做WordCount 的同时统计出特殊符号的总数
  */
object RDDDemo09_ShareVariable {
  def main(args: Array[String]): Unit = {
    //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //TODO 1. source/ 加载数据/ 创建RDD
    // 创建一个累加器/计数器
    val mycounter: LongAccumulator = sc.longAccumulator("mycounter")

    //定义一个特殊字符
    val rulesList = List(",",".","!","#","%","$" )
    //  RDD 一行行数据
    val lines: RDD[String] = sc.textFile("data/input/words2.txt")

    // 因为是分布式 需要 将集合作为广播变量广播出去到各个节点
    val broadcast = sc.broadcast(rulesList)

    val wordcountResult = lines.filter(StringUtils.isNotBlank(_)).
      flatMap(_.split("\\s+")). //  按照一个或者多个字符切
      filter(ch => {
      // 获取广播数据
      val list = broadcast.value
      if(list.contains(ch)){//如果是特殊字符
        //计数器+1
        mycounter.add(1)
        false
      }else{//是单词
        true
      }
       //
    }).map((_,1)).
      reduceByKey(_+_)

    //TODO 2. transformation

    //TODO 3. sink/输出/action
    wordcountResult.foreach(println)
    println(s"----------------------------------------------------")
    val chResult = mycounter.value
    println("特殊字符的数量:"+chResult)

  }
}

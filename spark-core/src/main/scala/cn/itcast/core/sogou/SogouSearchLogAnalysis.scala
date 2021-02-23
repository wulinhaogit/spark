package cn.itcast.core.sogou

import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  SogouQ日志分析
  *  数据: data/input/SogouQ.sample
  *  需求:  对SogouSearchLog 进行分词并统计如下指标
  *  1. 热门搜索词
  *  2. 用户热门搜索词(带上用户id)
  *  3. 各个时间段搜索热度
  *
  */
object SogouSearchLogAnalysis {
  def main(args: Array[String]): Unit = {
      // TODO 0. 准备环境
      val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
      val sc = new SparkContext(conf)
       sc.setLogLevel("WARN")
    //TODO 1.加载数据
    val  lines: RDD[String] = sc.textFile("data/input/SogouQ.sample") //2
    //TODO 2.处理封装数据
    val sogouRecordRDD: RDD[SogouRecord] = lines.map(line => {  //map一个进去一个出去
      val arr: Array[String] = line.split("\\s+")
      SogouRecord(
        arr(0),
        arr(1),
        arr(2),
        arr(3).toInt,
        arr(4).toInt,
        arr(5)
      )
    })





    //TODO 3.统计指标
     // 1. 热门搜索词
     // 切割数据
     val wordsRDD: RDD[String] = sogouRecordRDD.flatMap(record => { //flatMap 是一个进去多个出去(出去之后会被压扁(扁平化))  // 360安全卫士 ==> [360,安全卫士]
       val words: String = record.queryWords.replaceAll("\\[|\\]", "") // 360安全卫士
       ////将java 集合转换为scala集合
       import scala.collection.JavaConverters._
       HanLP.segment(words).asScala.map(_.word) // ArrayBuffer(360,安全卫士)

     })
    val result1 = wordsRDD
      .filter(word => !word.equals(".") && !word.equals("+"))
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .take(10)


    //  2. 用户热门搜索词(带上用户id)
    val userIdAndWordRDD: RDD[(String,String)] = sogouRecordRDD.flatMap(record => { //flatMap 是一个进去多个出去(出去之后会被压扁(扁平化))  // 360安全卫士 ==> [360,安全卫士]
      val wordsStr: String = record.queryWords.replaceAll("\\[|\\]", "") // 360安全卫士
      ////将java 集合转换为scala集合
      import scala.collection.JavaConverters._
      val words = HanLP.segment(wordsStr).asScala.map(_.word) // ArrayBuffer(360,安全卫士)
      val userId: String = record.userId
      words.map(word =>(userId , word))
    })
    val result2 = userIdAndWordRDD
      .filter(t => !t._2.equals(".") && !t._2.equals("+")).map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .take(10)
    
    
    // 3. 各个时间段搜索热度

    val result3 = sogouRecordRDD.map(record => {
      val hourAndMituntesStr = record.queryTime.substring(0, 5) //HH:mm
      (hourAndMituntesStr, 1)

    }).reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
    result3
    
    
    
    
    //TODO 4.输出结果
    println("--------------热门搜索词-------------------------")
     result1.foreach(println)
    println("------------------用户热门搜索词(带上用户id)---------------------")
    result2.foreach(println)
    println("------------------各个时间段搜索热度---------------------")
    result3.foreach(println)
    //TODO 5.释放资源
    sc.stop()
  }


  //准备一个样例类用来封账数据
  /**
    *
    * @param queryTime 访问时间 , 格式为HH:mm:ss
    * @param userId   用户ID
    * @param queryWords  查询词
    * @param resultRank   该URL在返回结果中的排名
    * @param clickRank  用户点击的顺序号
    * @param clickUrl 用户点击的URL
    */
  case class SogouRecord(queryTime: String,userId: String,queryWords: String,
                         resultRank: Int, clickRank: Int, clickUrl: String)
}

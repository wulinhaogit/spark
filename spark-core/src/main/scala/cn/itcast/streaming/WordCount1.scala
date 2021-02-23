package cn.itcast.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *  使用SparkStreaming 接收node1:9999的数据并做wordcount
  */
object WordCount1 {
  def main(args: Array[String]): Unit = {
    //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    //TODO 1. source/ 加载数据/ 创建RDD
    // SparkContext 和 批的间隔  每隔5秒划分一个批次
    val ssc = new StreamingContext(sc,Seconds(5))
    sc.setLogLevel("WARN")
    //TODO 2. 加载数据
    val lines = ssc.socketTextStream("node1",9999 )
    // TODO 3. 梳理数据
    val resoutDS = lines.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey((_ + _))

    //TODO 4. sink/输出
    resoutDS.print()

    //TODO 4. 启动并等待结束
    ssc.start()
    ssc.awaitTermination()//注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来
    
    //TODO 5. 关闭资源
       ssc.stop(stopSparkContext = true,stopGracefully = true)
  }
}

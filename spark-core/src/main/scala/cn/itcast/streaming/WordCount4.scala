package cn.itcast.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  使用SparkStreaming 接收node1:9999的数据并做wordcount+窗口计算
  *
  *  每隔5秒计算最近10秒的数据
  */
object WordCount4 {
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
      .map((_,1))
      //windowDuration: Duration, 窗口的大小  ,表示计算最近多长时间的数据
      // slideDuration: Duration  滑动的间隔  表示每隔多长时间计算一次
      // 注意 windowDuration 和 slideDuration 必须是 批间隔的倍数
      // 每隔5S(滑动时间)计算最近10S(窗口长度/窗口大小)的数据
      .reduceByKeyAndWindow((a:Int,b:Int)=>{a+b},Seconds(10),Seconds(5))
    //TODO 4. sink/输出
    resoutDS.print()

    //TODO 4. 启动并等待结束
    ssc.start()
    ssc.awaitTermination()//注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来
    
    //TODO 5. 关闭资源
       ssc.stop(stopSparkContext = true,stopGracefully = true)
  }
}

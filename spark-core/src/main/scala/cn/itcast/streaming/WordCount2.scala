package cn.itcast.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  *  使用SparkStreaming 接收node1:9999的数据并做wordcount+实现状态管理
  *   如: 每批次输入一个spark hadoop,第一次得到   (spark,1) (hadoop,1)
  *   在下一个批次  spark  spark    得到  (spark,3) (hadoop,1)
  */
object WordCount2 {
  def main(args: Array[String]): Unit = {
    //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    //TODO 1. source/ 加载数据/ 创建RDD
    // SparkContext 和 批的间隔  每隔5秒划分一个批次
    val ssc = new StreamingContext(sc,Seconds(5))
    sc.setLogLevel("WARN")

    //注意state存在checkpoint中
    ssc.checkpoint("./ckp")
    //TODO 2. 加载数据
    val lines = ssc.socketTextStream("node1",9999 )
    // TODO 3. 处理数据
    //定义一个函数用来处理状态: 把当前数据和历史数据累加
    /**
      * currentValues: 表示该key(如:spark)的当前批次的值  如:[1,1]
      * historyValues: 表示该key(如:spark)的历史值,第一次是0后面就是之前的累加值  如 1
      */
    val updateFunc =(currentValues:Seq[Int],historyValues:Option[Int]) => {
        if (currentValues.size>0){
           val currentResult: Int=  currentValues.sum+historyValues.getOrElse(0)
           Some(currentResult)
        }else{
          historyValues
        }
    }

    val resoutDS = lines.flatMap(_.split(" "))
      .map((_, 1))
      //updateFunc: (Seq[V], Option[S]) => Option[S]
        .updateStateByKey(updateFunc)

    //TODO 4. sink/输出
    resoutDS.print()

    //TODO 4. 启动并等待结束
    ssc.start()
    ssc.awaitTermination()//注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来
    
    //TODO 5. 关闭资源
       ssc.stop(stopSparkContext = true,stopGracefully = true)
  }
}

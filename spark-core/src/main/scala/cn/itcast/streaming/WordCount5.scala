package cn.itcast.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  使用SparkStreaming 接收node1:9999的数据并做wordcount+窗口计算
  *   模拟百度热搜排行榜 每隔10S计算最近20S的热搜词
  */
object WordCount5 {
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
      // 每隔10S计算最近20S的热搜词
      .reduceByKeyAndWindow((a:Int,b:Int)=>{a+b},Seconds(20),Seconds(10))


      //DStream 没有提出提供直接排序的方法需要对底层的RDD进行操作
    //DStream 的 transform 方法表示对DStream底层的RDD进行操作并返回结果
    val sortRDD = resoutDS.transform(rdd => {
      val sortRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      val top3 = sortRDD.take(3)


      println("=====top3======")
      top3.foreach(println)
      println("=====top3======")
      sortRDD
    })


    //TODO 4. sink/输出
    sortRDD.print()

    //TODO 4. 启动并等待结束
    ssc.start()
    ssc.awaitTermination()//注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来
    
    //TODO 5. 关闭资源
       ssc.stop(stopSparkContext = true,stopGracefully = true)
  }
}


/*

  31省区市新增7例均为境外输入 31省区市新增7例均为境外输入 31省区市新增7例均为境外输入 31省区市新增7例均为境外输入
  英国就新冠源头抹黑中国中方回应热 英国就新冠源头抹黑中国中方回应热
  节后上班第一天 这六件事要记住
  BBC报道中国使用"阴间滤镜"
  中疾控披露吉林"讲师1传141"细节
  沈腾成为中国影史票房第一的演员
  金正恩夫人时隔1年再次公开露面
  民法典解答孩子红包能否自己保管新
  网红医生用麻醉药捂晕自己后道歉
  张小斐谈背贾玲好温柔 张小斐谈背贾玲好温柔 张小斐谈背贾玲好温柔
  *
  */

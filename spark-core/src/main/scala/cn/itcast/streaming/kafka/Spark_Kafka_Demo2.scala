package cn.itcast.streaming.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 演示使用 spark-Streaming-kafka-0.10_2.12中的Direct模式连接kafka消费数据  手动提交偏移量
  */
object Spark_Kafka_Demo2 {

  def main(args: Array[String]): Unit = {
    //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // SparkContext 和 批的间隔  每隔5秒划分一个批次
    val ssc = new StreamingContext(sc,Seconds(5))
    //设置 checkpoint
    ssc.checkpoint("./ckp")


    //TODO 1. source 加载数据 从kafka

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "single:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sparkDemo",
      "auto.offset.reset" -> "latest",
      //"auto.commit.interval.ms" ->"1000",  //自动提交的时间间隔
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

   // val topics = Array("topicA", "topicB")
    val topics = Array("spark_kafka")  //订阅的主题
    val kafkaDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //位置策略使用源码中推荐的
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams) //消费策略使用源码中推荐的
    )

    // TODO 处理消息
    //注意提交的时机:应该是消费完一批就提交一次offset 而再DStream 一小批的提交时RDD
    kafkaDS.foreachRDD( rdd=>{
      if(!rdd.isEmpty()){
        rdd.foreach(record => {
          val topic = record.topic()
          val partition = record.partition()
          val offset = record.offset()
          val key = record.key()
          val value = record.value()
          val info = s"""topic:${topic},partition:${partition},offset:${offset},key:${key},value:${value}"""

          println("消费到的详细信息为: "+info)
        })
        //获取rdd中offset相关的信息 offsetRanges里面就包含了改批次各个分区的offset信息
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //提交
        kafkaDS.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        println("当前批次的数据已经消费并手动提交")
      }
    }
    )


    //TODO 4. 启动并等待结束
    ssc.start()
    ssc.awaitTermination()//注意:流式应用程序启动之后需要一直运行等待手动停止/等待数据到来

    //TODO 5. 关闭资源
    ssc.stop(stopSparkContext = true,stopGracefully = true)


  }

}

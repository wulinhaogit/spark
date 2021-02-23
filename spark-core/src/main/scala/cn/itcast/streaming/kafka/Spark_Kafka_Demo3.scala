package cn.itcast.streaming.kafka

import java.sql.DriverManager

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * 演示使用 spark-Streaming-kafka-0.10_2.12中的Direct模式连接kafka消费数据  手动提交偏移量到MYSQL
  */
object Spark_Kafka_Demo3 {

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

    // 从数据库 获取 这个消费者组这个topic 的 偏移量map
    val offsetMap = OffsetUtil.getOffsMap("sparkDemo","spark_kafka")
    println("---------------------------------------------------------------------------------------")

    val kafkaDS = if (offsetMap.size>0){
      println("Mysql中存储了这个消费者组这个topic 的 偏移量记录,接下来用 offser记录初来时消费")
     KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent, //位置策略使用源码中推荐的
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,offsetMap) //消费策略使用源码中推荐的
      )
    }else{
      println("Mysql中没有存储这个消费者组这个topic 的 偏移量记录,接下从latest开始消费 ")
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent, //位置策略使用源码中推荐的
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams) //消费策略使用源码中推荐的
      )
    }



    // TODO 处理消息
    //注意提交的时机:应该是消费完一批就提交一次offset 而再DStream 一小批的提交时RDD
    kafkaDS.foreachRDD( rdd=>{
      //在这里获取redis的信息

      //rdd to ds/df

      //获取字段信息

      //foreach 进入 ck
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
       // kafkaDS.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        //收到提交到mysql
        OffsetUtil.saveOffsetRanges("sparkDemo",offsetRanges)
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


  /**
    * 存取偏移量的工具类
    */
  object OffsetUtil{

    //保存偏移量到数据库
    def saveOffsetRanges(groupid:String, offsetRanges: Array[OffsetRange]) ={
      // 开启连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sys?useUnicode=true&characterEncoding=utf-8&useSSL=false","root","root")
      // replace into 标识之前有就替换没有就插入
      val sql="replace into t_offset (`topic`,`partition`,`groupid`,`offset`) values (?,?,?,?);"
      val ps = conn.prepareStatement(sql)
      for (o<- offsetRanges) {
        ps.setString(1,o.topic)
        ps.setInt(2,o.partition)
        ps.setString(3,groupid)
        ps.setLong(4,o.untilOffset)
        ps.executeUpdate()
      }
      //关闭连接
      ps.close()
      conn.close()

    }


    //从数据库读取偏移量map
    def getOffsMap(groupid:String, topic: String) ={
      // 开启连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sys?useUnicode=true&characterEncoding=utf-8&useSSL=false","root","root")
      // replace into 标识之前有就替换没有就插入
      val sql="select * from t_offset where groupid=? and topic=?;"
      val ps = conn.prepareStatement(sql)
      ps.setString(1,groupid)
      ps.setString(2,topic)
      //执行查询
      val rs = ps.executeQuery()
      // 定义Map[主题分区,offset]
      val offsetMap = mutable.Map[TopicPartition,Long]()
      // 遍历结果集 放入
      while (rs.next()){
        offsetMap += new TopicPartition(rs.getString("topic"),rs.getInt("partition"))  -> rs.getLong("offset")
      }
      //关闭连接
      ps.close()
      conn.close()

      offsetMap
    }


  }

}

package cn.edu.analysis.streaming

import java.util.Properties

import cn.edu.bean.Answer
import cn.edu.utils.RedisUtil
import com.google.gson.Gson
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.{SparkContext, streaming}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * c从kafka消费消息(消息中由用户id),然后redis中获取推荐模型的路径,并从路径中加载模型推荐模型ALSModel
  * 然后使用该模型给用户推荐易错题
  */

object StreamingRecommend {
  def main(args: Array[String]): Unit = {
    //TODO 1.主备环境

    val spark: SparkSession = SparkSession.builder().appName("StreamingAnalysis").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val ssc = new StreamingContext(sc,streaming.Seconds(5))

    //TODO 2.加载数据

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "single:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sparkDemo",
      "auto.offset.reset" -> "latest",
      "auto.commit.interval.ms" ->"1000",  //自动提交的时间间隔
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    // val topics = Array("topicA", "topicB")
    val topics = Array("edu")  //订阅的主题
    val kafkaDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //位置策略使用源码中推荐的
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams) //消费策略使用源码中推荐的
    )


    //TODO 3. 处理数据
    val valueDS = kafkaDS.map(record => {
      record.value()
    })
    valueDS.foreachRDD(rdd=>{
      if (!rdd.isEmpty()){
        //该rdd表示每个微批的数据
        //==1. 获取path并加载模型
        //获取redis连接
        val jedis = RedisUtil.pool.getResource
        //加载模型路径
        val path = jedis.hget("als_model","recommended_question_id")
        //根据路径加载模型
        val model = ALSModel.load(path)

        //==2.取出用户id
        val answerDF = rdd.coalesce(1).map(json => {
          val gson = new Gson()
          gson.fromJson(json, classOf[Answer])
        }).toDF
        //将用户id转为数字,因为后续模型推荐的时候需要数字格式的Id
        val id2int = udf((student_id: String) => {
          student_id.split("_")(1).toInt
        })

        val studentIdDF = answerDF.select(id2int($"student_id") as "student_id")

        //==3. 使用模型给用户推荐题目
        val recommendDF = model.recommendForUserSubset(studentIdDF,10)
        recommendDF.printSchema()
        /*
          root
            |-- student_id: integer (nullable = false)  用户id
            |-- recommendations: array (nullable = true) 推荐列表
            |    |-- element: struct (containsNull = true)
            |    |    |-- question_id: integer (nullable = true)  题目id
            |    |    |-- rating: float (nullable = true) 评分/推荐指数
         */
        recommendDF.show(false)
        /*
        +---------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |student_id 用户id    |recommendations            题目id,  分/推荐指数                                                                                                                                                                     |
        +---------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |12                   |[[1225, 1.913353], [60, 1.1508352], [1562, 0.8117588], [984, 0.7853798], [1656, 0.7853798], [902, 0.7613853], [45, 0.7480976], [224, 0.7064658], [1083, 0.67618763], [198, 0.55118316]]           |
        |14                   |[[198, 0.9099754], [224, 0.72415733], [1318, 0.71683264], [201, 0.6653093], [1890, 0.5906269], [1225, 0.5726685], [60, 0.5707907], [1562, 0.56900674], [55, 0.56781083], [45, 0.56464404]]        |
         */


        //处理推荐结果: 取出用户id和题目id:  拼接成字符串: "id1,id2,...
        val recommendResultDF = recommendDF.as[(Int, Array[(Int, Float)])].map(t => {
          val studentIdStr = "学生ID_" + t._1
          val questionIdStr = t._2.map("题目_" + _._1).mkString(",")
          (studentIdStr, questionIdStr)
        }).toDF("student_id", "recommendations")

        //将answerDF和recommendResultDF  进行 join
        val allInfoDF = answerDF.join(recommendResultDF,"student_id")

        //===4.输出结果到mysql
        if (allInfoDF.count()>0){
          val properties = new  Properties()
          properties.setProperty("user","root")
          properties.setProperty("password","root")
          allInfoDF.write.mode(SaveMode.Append).jdbc(
            "jdbc:mysql://localhost:3306/sys?useUnicode=true&characterEncoding=utf-8&useSSL=false",
            "t_recommended",
            properties
          )
        }

        // 关闭redis连接
        jedis.close()
      }

    })



    //TODO 4. 输出结果
    //TODO 5. 启动并等待结束
    ssc.start()
    ssc.awaitTermination()


    //TODO 6. 关闭资源
    spark.stop()
  }

}

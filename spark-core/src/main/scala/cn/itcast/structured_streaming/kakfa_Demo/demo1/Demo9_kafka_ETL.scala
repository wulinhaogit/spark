package cn.itcast.structured_streaming.kakfa_Demo.demo1

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  StructuredStreaming整合kafka实时案例 -----   1.实时数据ETL-案例
  */
object Demo9_kafka_ETL {
  def main(args: Array[String]): Unit = {
      //TODO 1.创建环境
      //因为演示StructuredStreaming 基于SparkSQL的且编程API/数据抽象是DF/DS,所以这里创建SparkSessionjike
      val spark = SparkSession.builder().appName("sparksql").master("local[*]")
        .config("spark.sql.shuffle.partitions","4") // 本次测试时将分区数设置的小一点,实际开发中可以根据集群的规模调整大小,默认 200
        .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")
      import spark.implicits._
      //TODO 2.加载数据   kafka   - stationTopic
    val kafkaDF:DataFrame= spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","single:9092")
      .option("subscribe", "stationTopic")
      .load()
    val valueDS = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]  //as[String]  将df转为DS

      //TODO 3.处理数据 ETL--  过滤出success的数据
    val etlResult = valueDS.filter(_.contains("success"))

      //TODO 4.输出结果  kafka ---- etlTopic
    etlResult.writeStream
      .format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers","single:9092")
      .option("topic", "etlTopic")
      .option("checkpointLocation","./ckp"+System.currentTimeMillis())  //设置 checkpoint 地址
      .trigger(Trigger.Continuous("1 second"))
      .start().awaitTermination()

      //TODO 6.关闭资源
    spark.stop()


  }
}

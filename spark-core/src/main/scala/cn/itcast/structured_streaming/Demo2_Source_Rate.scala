package cn.itcast.structured_streaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *  演示StructuredStreaming 的Rate source
  */
object Demo2_Source_Rate {
  def main(args: Array[String]): Unit = {
      //TODO 1.创建环境
      //因为演示StructuredStreaming 基于SparkSQL的且编程API/数据抽象是DF/DS,所以这里创建SparkSessionjike
      val spark = SparkSession.builder().appName("sparksql").master("local[*]")
        .config("spark.sql.shuffle.partitions","4") // 本次测试时将分区数设置的小一点,实际开发中可以根据集群的规模调整大小,默认 200
        .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel("WARN")
      import spark.implicits._
      //TODO 2.加载数据
    val df:DataFrame= spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")//每秒生的数据条数
      .option("rampUpTime", "0s") //每条数据生成的时间
      .option("numPartitions", "2")//分区数目
      .load()

    df.printSchema()

      //TODO 3.处理数据

      //TODO 4.输出结果
    df.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate",false)//标识队列不进行阶段.也就是内容全部展示
      //TODO 5.启动并等待结束
      .start().awaitTermination()

      //TODO 6.关闭资源
    spark.stop()


  }
}

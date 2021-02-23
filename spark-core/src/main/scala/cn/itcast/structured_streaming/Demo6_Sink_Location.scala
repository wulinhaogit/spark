package cn.itcast.structured_streaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *  演示StructuredStreaming 的 sink 输出到  控制台和 内存的打印
  */
object Demo6_Sink_Location {
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
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

      //TODO 3.处理数据
    val ds:Dataset[String] = df.as[String]
    val result = ds.flatMap(_.split(" "))
      .groupBy('value)
      .count()
      .orderBy('count.desc)

      //TODO 4.输出结果
  /*  result.writeStream
      .format("console")
      .outputMode("complete")
      //TODO 5.启动并等待结束
      .start()awaitTermination() */

    //输出到内存
    val query = result.writeStream
      .format("memory") //输出到内存
      .outputMode("complete")
      .queryName("tableName")
      //TODO 5.启动
      .start()


    while (true){
      spark.sql("select * from tableName" ).show()  //这里必须.show要不没有反应
    }
    //

    //TODO 6. 并等待结束
  //  query.awaitTermination()  上面循环了这里就不要了
      //TODO 6.关闭资源
    spark.stop()


  }
}

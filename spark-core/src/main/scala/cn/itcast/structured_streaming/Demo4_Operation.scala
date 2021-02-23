package cn.itcast.structured_streaming

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *  演示StructuredStreaming 的 Operation  操作
  */
object Demo4_Operation {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建环境
    //因为演示StructuredStreaming 基于SparkSQL的且编程API/数据抽象是DF/DS,所以这里创建SparkSessionjike
    val spark = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions","4") // 本次测试时将分区数设置的小一点,实际开发中可以根据集群的规模调整大小,默认 200
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //TODO 2.加载数据
    val df:DataFrame= spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

    //TODO 3.处理数据
    //TODO  =================SDL========================
    val ds:Dataset[String] = df.as[String]
    val wordsDS = ds.flatMap(_.split(" "))
    val result = wordsDS
      .groupBy('value)
      .count()
      .orderBy('count.desc)


    //TODO  =================SQL========================
    wordsDS.createOrReplaceTempView("t_words")
    var sql =
      """
        |select value,count(*) as counts
        |from t_words
        |group by value
        |order by counts desc
      """.stripMargin
    val result2 = spark.sql(sql)

    //TODO 4.输出结果
    result.writeStream
      .format("console")
      .outputMode("complete")
      //TODO 5.启动并等待结束
      .start()
    //.awaitTermination()  //后面还有代码要执行所以要注释掉



    result2.writeStream
      .format("console")
      .outputMode("complete")
      //TODO 5.启动并等待结束
      .start()
      .awaitTermination()

    //TODO 6.关闭资源
    spark.stop()


  }
}

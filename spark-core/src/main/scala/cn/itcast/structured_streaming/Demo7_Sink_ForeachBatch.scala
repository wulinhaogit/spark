package cn.itcast.structured_streaming

import org.apache.spark.sql._

/**
  *  演示StructuredStreaming 的socket source
  */
object Demo7_Sink_ForeachBatch {
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

    //输出到内存
    val query = result.writeStream
      .foreachBatch((ds:Dataset[Row], batchId:Long)=>{
        //自定义输出到控制台
        println("------------------------")
        println(batchId)
        println("------------------------")
        ds.show()

        //自定义输出到Mysql
        ds.coalesce(1)
          .write.mode(SaveMode.Overwrite)
          .format("jdbc")
          //.option("driver","com.mysql.cj.jdbc.Driver") // mysql 8.0+
          //.option("url","jdbc:mysql://localhost:3306/sys?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC")  //mysql 8.0+
          .option("url","jdbc:mysql://localhost:3306/sys?useUnicode=true&characterEncoding=utf-8&useSSL=false")
          .option("user","root")
          .option("password","root")
          .option("dbtable","sys.t_struct_words") //没有表会自动创建
          .save()

    })//每一批
      .outputMode("complete")
      .queryName("tableName")
      //TODO 5.启动
      .start()




    //TODO 6. 并等待结束
   query.awaitTermination()
      //TODO 6.关闭资源
    spark.stop()


  }
}

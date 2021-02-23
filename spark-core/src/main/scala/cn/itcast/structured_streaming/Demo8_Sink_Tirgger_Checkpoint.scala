  package cn.itcast.structured_streaming

  import org.apache.spark.sql.streaming.Trigger
  import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

  /**
    *  演示StructuredStreaming 的socket source
    */
  object Demo8_Sink_Tirgger_Checkpoint {
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
      val result = ds.coalesce(1).flatMap(_.split(" "))
        .groupBy('value)
        .count()
        .orderBy('count.desc)

        //TODO 4.输出结果
      /*
      // Default trigger (runs micro-batch as soon as it can) (默认(不写)的  0 seconds)
      df.writeStream
        .format("console")
        .start()

      // ProcessingTime trigger with two-seconds micro-batch interval  指定间隔微批次 2 seconds
      df.writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime("2 seconds"))
        .start()

      // One-time trigger 一次性微批量
      df.writeStream
        .format("console")
        .trigger(Trigger.Once())
        .start()
  */
      // Continuous trigger with one-second checkpointing interval 具有指定检查点间隔的连续处理 （实验性的  尽量不用）
      // 需要设置 checkpoint 地址
      //df 必须 设置分区 coalesce(1)
      df.writeStream
        .format("console")
        .trigger(Trigger.Continuous("1 second"))
        .option("checkpointLocation","./ckp"+System.currentTimeMillis())  //设置 checkpoint 地址
        .start()

        //TODO 6.关闭资源
      spark.stop()


    }
  }

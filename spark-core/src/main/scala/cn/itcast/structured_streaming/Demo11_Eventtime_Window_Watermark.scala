  package cn.itcast.structured_streaming

import java.sql.Timestamp

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

  /**
    *  演示StructuredStreaming     基于事件时间进行窗口计算 + Watermaker水位线/水印解决数据延迟到达问题
    *  (能够允许一定时间的数据迟到, 迟到严重的直接抛弃)
    */
  object Demo11_Eventtime_Window_Watermark {
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
      val socketDF:DataFrame= spark.readStream
        .format("socket")
        .option("host", "node1")
        .option("port", 9999)
        .load()

        //TODO 3.处理数据: 添加schema
      val wordDF  = socketDF.as[String]
        .filter(StringUtils.isNotBlank(_))
        .map(line=>{
          val arr = line.trim.split(",")
          val timestampStr = arr(0)
          val word = arr(1)
         (Timestamp.valueOf(timestampStr),word)
        })
        //设置列名称
        .toDF("timestamp","word")

      //需求 每隔5秒计,算最近10秒的数据,withWatermark设置为 10S

      val windowedCounts = wordDF
      //withWatermark(指定的事件时间是哪一列,指定的时间阈值)
        .withWatermark("timestamp","10 seconds")
        .groupBy(
          //指定基于事件时间做窗口聚合计算,WordCount
          //window(指定事件时间是哪一列,窗口长度,滑动间隔)

          window($"timestamp", "10 minutes", "5 minutes"),
          $"word"
        ).count()


        //TODO 4.输出结果
      windowedCounts.writeStream
        .format("console")
        .outputMode(OutputMode.Update())
        .option("truncate","false")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start().awaitTermination()

        //TODO 6.关闭资源
      spark.stop()


    }
  }
//nc -lk 9999 启动端口
  /*

2019-10-10 12:00:07,dog
2019-10-10 12:00:08,owl

2019-10-10 12:00:14,dog
2019-10-10 12:00:09,cat

2019-10-10 12:00:15,cat
2019-10-10 12:00:08,dog     --迟到不严重,参与计算,影响结果
2019-10-10 12:00:13,owl
2019-10-10 12:00:21,owl

2019-10-10 12:00:04,donkey   --迟到严重,参与计算,影响结果
2019-10-10 12:00:17,owl      --迟到不严重,参与计算,影响结果


    */
  package cn.itcast.structured_streaming

import java.sql.Timestamp

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

  /**
    *  演示StructuredStreaming    Strctured Streaming 流数据去重 (Deduplication)
    *
    * * 需求
    *
    *
    * 对网站用户的日志数据,按照userId和eventType去重统计
    * 数据如下:
    *
    * {"eventTime":"2016-01-10 10:01:50","eventType":"browse","userID":"1"}
    * {"eventTime":"2016-01-10 10:01:50","eventType":"click","userID":"1"}
    * {"eventTime":"2016-01-10 10:01:50","eventType":"slide","userID":"1"}
    * {"eventTime":"2016-01-10 10:01:50","eventType":"browse","userID":"1"}
    * {"eventTime":"2016-01-10 10:01:50","eventType":"click","userID":"1"}
    * {"eventTime":"2016-01-10 10:01:50","eventType":"slide","userID":"1"}
    */
  object Demo12_Deduplication {
    def main(args: Array[String]): Unit = {
        //TODO 1.创建环境
        //因为演示StructuredStreaming 基于SparkSQL的且编程API/数据抽象是DF/DS,所以这里创建SparkSessionjike
        val spark = SparkSession.builder().appName("sparksql").master("local[*]")
          .config("spark.sql.shuffle.partitions","4") // 本次测试时将分区数设置的小一点,实际开发中可以根据集群的规模调整大小,默认 200
          .getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("WARN")
        import org.apache.spark.sql.functions._
        import spark.implicits._


      //TODO 2.加载数据
      val socketDF:DataFrame= spark.readStream
        .format("socket")
        .option("host", "node1")
        .option("port", 9999)
        .load()

        //TODO 3.处理数据: 添加schema
      val schemaDF = socketDF.as[String]
        .filter(StringUtils.isNotBlank(_))
        .select(
          get_json_object($"value", "$.eventTime").as("eventTime"),
          get_json_object($"value", "$.eventType").as("eventType"),
          get_json_object($"value", "$.userID").as("userID")
        )


      val result:Dataset[Row] = schemaDF
        .dropDuplicates("eventTime","eventType")
        .groupBy("userID").count()
        //TODO 4.输出结果
      result.writeStream
        .format("console")
        .outputMode(OutputMode.Update())
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
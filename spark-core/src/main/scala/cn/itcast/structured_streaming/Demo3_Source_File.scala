package cn.itcast.structured_streaming

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *  演示StructuredStreaming 的File source
  */
object Demo3_Source_File {
  def main(args: Array[String]): Unit = {
      //TODO 1.创建环境
      //因为演示StructuredStreaming 基于SparkSQL的且编程API/数据抽象是DF/DS,所以这里创建SparkSessionjike
      val spark = SparkSession.builder().appName("sparksql").master("local[*]")
        .config("spark.sql.shuffle.partitions","4") // 本次测试时将分区数设置的小一点,实际开发中可以根据集群的规模调整大小,默认 200
        .getOrCreate()
      val sc = spark.sparkContext
     //定义schema
    val CSVSchema = new StructType()
      .add("name", StringType, nullable = true)
      .add("age", StringType, nullable = true)
      .add("hobby", StringType, nullable = true)


      sc.setLogLevel("WARN")
    import spark.implicits._
      //TODO 2.加载数据
    val df:DataFrame= spark.readStream
      .option("sep", ";")//跟什么什么分割
      .option("header", false) //每条数据生成的时间
      .schema(CSVSchema) //注意 : 流式处理对与结构化数据哪怕是有约束也需要单独指定
      .format("csv").load("data/input/persons")//.csv("data/input/persons")


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

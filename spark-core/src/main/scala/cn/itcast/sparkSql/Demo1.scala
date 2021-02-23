package cn.itcast.sparkSql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 演示SparkSql 初级案例
  */
object Demo1 {
  def main(args: Array[String]): Unit = {
    //TODO  0. 准备环境
    val spark = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //TODO 1. 加载数据
    val ds: Dataset[String] = spark.read.textFile("data/input/words.txt")
    val df2: DataFrame= spark.read.json("data/input/json")
   // val df3: DataFrame= spark.read.csv("data/input/scv")
    //TODO 2. 处理数据

    //TODO 3.输出结果
    ds.printSchema()
    df2.printSchema()
    ds.show()
    df2.show()
    //TODO 关闭资源
    spark.stop()

  }
}

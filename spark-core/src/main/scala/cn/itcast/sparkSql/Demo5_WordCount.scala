package cn.itcast.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 演示SparkSql   SQL 和 DSL两种编程方式
  *
  */
object Demo5_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO  0. 准备环境
    val spark = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //TODO 1. 加载数据

    val df: DataFrame = spark.read.text("data/input/words.txt")
    val ds: Dataset[String] = spark.read.textFile("data/input/words.txt")
    //TODO 2. 处理数据

    //df.flatMap(_.split("\\s+"))  // df没有泛型不能直接使用split
    import spark.implicits._
    val words: Dataset[String] = ds.flatMap(_.split("\\s+"))

    //TODO  ================SQL编程=================
    words.createOrReplaceTempView("t_words")
    var sql =
      """
        |select value,count(*) as counts
        |from t_words
        |group by value
        |order by counts desc
      """.stripMargin
    spark.sql(sql).show()

    //TODO  ================DSL编程=================
   words.groupBy($"value").count().orderBy($"count".desc).show()




    //TODO 3.输出结果





    //TODO 关闭资源
    spark.stop()

  }



}

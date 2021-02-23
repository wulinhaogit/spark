package cn.itcast.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 演示SparkSql   RDD  DF DS 的 相互转换
  */
object Demo3_RDD_DF_DS {
  def main(args: Array[String]): Unit = {
    //TODO  0. 准备环境
    val spark = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //TODO 1. 加载数据
    val lines: RDD[String] = sc.textFile("data/input/person.txt")
    //TODO 2. 处理数据
    val personRDD: RDD[Person] = lines.map(line => {
      val arr: Array[String] = line.split("\\s+")
      Person(arr(0).toInt, arr(1), arr(2).toInt)
    })
   
    
    // 转换 1 RDD转为DataFrame
    import spark.implicits._
    val personDataFrame:DataFrame  = personRDD.toDF()

    //转换 2 RDD转为DataSet
    import spark.implicits._
    val personDataSet:Dataset[Person]  = personRDD.toDS()

    //转换 3 DF转为RDD  注意RDD没有泛型, 转为RDD时用的是Row
    val rdd1:RDD[Row] = personDataFrame.rdd

    //转换4 DS转为RDD
    val rdd2:RDD[Person] = personDataSet.rdd


    //转换 5 DF转为DS
    val ds:Dataset[Person] = personDataFrame.as[Person]


    //转换4 DS转为DF
    val df:DataFrame = personDataSet.toDF()


    //TODO 3.输出结果
    personDataFrame.printSchema()
    personDataFrame.show()

    personDataSet.printSchema()
    personDataSet.show()

    rdd1.foreach(println)

    rdd2.foreach(println)

    ds.printSchema()
    ds.show()

    df.printSchema()
    df.show()



    //TODO 关闭资源
    spark.stop()

  }


  case class Person(id:Int,name:String,age:Int)
}

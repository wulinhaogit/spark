package cn.itcast.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 演示SparkSql    自定义schema:  RDD 转为 DataFrame
  */
object Demo2_RDDToDataFrame3 {
  def main(args: Array[String]): Unit = {
    //TODO  0. 准备环境
    val spark = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //TODO 1. 加载数据
    val lines: RDD[String] = sc.textFile("data/input/person.txt")
    //TODO 2. 处理数据
    val rowRDD: RDD[Row] = lines.map(line => {
      val arr: Array[String] = line.split("\\s+")
      Row(arr(0).toInt, arr(1), arr(2).toInt)
    })
   
    
    //RDD转为DataFrame
    import spark.implicits._

   /* val schema =
         StructType(
             StructField("id", IntegerType, false) ::
               StructField("name", StringType, false) ::
               StructField("age", IntegerType, false) :: Nil)*/

    val schema =
      StructType(List(
        StructField("id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("age", IntegerType, false)))
    val personDataFrame:DataFrame  =  spark.createDataFrame(rowRDD,schema)


    //TODO 3.输出结果
    personDataFrame.printSchema()
    personDataFrame.show()


    //TODO 关闭资源
    spark.stop()

  }



}

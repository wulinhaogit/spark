package cn.itcast.sparkSql

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * 演示SparkSql 支持外部数据源
  *
  * 支持的文件格式 : text/json/csv/parqute/orc...
  * 支持文件系统数据库
  *
  */
object Demo6_Datasource {
  def main(args: Array[String]): Unit = {


    val prop = new  Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")


    //TODO  0. 准备环境
    val spark = SparkSession.builder().appName("sparksql").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //TODO 1. 从外部加载数据

    val df: DataFrame = spark.read.json("data/input/json")
    //val df: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/sys?useUnicode=true&characterEncoding=utf-8&useSSL=false","person",prop)

    df.show()
    df.printSchema()

    //TODO 3.输出到外部
    df.coalesce(1).write.json("data/output/json")
    df.coalesce(1).write.csv("data/output/csv")
    df.coalesce(1).write.orc("data/output/orc")
    df.coalesce(1).write.parquet("data/output/parquet")


    df.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://localhost:3306/sys?useUnicode=true&characterEncoding=utf-8&useSSL=false","person",prop)





    //TODO 关闭资源
    spark.stop()

  }


}

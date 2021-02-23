package cn.itcast.sparkSql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * 演示SparkSql :  使用SparkSQL整合Hive
  *
  *
  */
object Demo9_Hive {
  def main(args: Array[String]): Unit = {


    //TODO  0. 准备环境
    val spark = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions","4") // 本次测试时将分区数设置的小一点,实际开发中可以根据集群的规模调整大小,默认 200
      .config("spark.sql.warehouse.dir","hdfs://node1:8020/user/hive/warehouse")//知道hive数据库在hdfs上的位置
      .config("hive.metastore.uris","thirft://node2:9083") //hive的启动地址
      .enableHiveSupport()//开启对hive语法的支持
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    //TODO 1. 操作hive

    spark.sql("show database").show(false)
    spark.sql("show tables").show(false)


    //TODO 关闭资源
    spark.stop()
  }
}

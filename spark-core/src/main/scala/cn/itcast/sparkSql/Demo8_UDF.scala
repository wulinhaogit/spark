package cn.itcast.sparkSql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 演示SparkSql :  使用SparkSQL UDF udf.txt 的数据转为大写
  *
  *
  */
object Demo8_UDF {
  def main(args: Array[String]): Unit = {


    //TODO  0. 准备环境
    val spark = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions","4") // 本次测试时将分区数设置的小一点,实际开发中可以根据集群的规模调整大小,默认 200
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    //TODO 1. 从外部加载数据

    val ds: Dataset[String] = spark.read.textFile("data/input/udf.txt")
    ds.show()
    ds.printSchema()
    //TODO 2. 处理数据

    //TODO ==================SQL===============
   ds.createOrReplaceTempView("t_udf")

    //TODO 自定义UDF函数
    spark.udf.register("small2big",(value:String)=>{
      value.toUpperCase()
    })

    val sql=
      """
        |select value,small2big(value) as bigValue
        |from t_udf
      """.stripMargin
    spark.sql(sql).show()


    //TODO ==================SDL================
    //TODO 自定义UDF函数
    import org.apache.spark.sql.functions._
    var small2big =udf((value:String) =>{
      value.toUpperCase()
    })

    ds.select('value,small2big('value).as("bigValue")).show()
    //TODO 关闭资源
    spark.stop()
  }
}

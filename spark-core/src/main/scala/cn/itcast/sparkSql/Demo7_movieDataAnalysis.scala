package cn.itcast.sparkSql



import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 演示SparkSql 对电影评分数据进行统计分析,分别使用DSL编程和SQL编程,获取电影平均分Top3,要求电影 评分次数大于6
  *
  */
object Demo7_movieDataAnalysis {
  def main(args: Array[String]): Unit = {


    //TODO  0. 准备环境
    val spark = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions","4") // 本次测试时将分区数设置的小一点,实际开发中可以根据集群的规模调整大小,默认 200
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    //TODO 1. 从外部加载数据

    val ds: Dataset[String] = spark.read.textFile("data/input/movie")
    //TODO 2. 处理数据
    val movieDF: DataFrame = ds.map(line => {
      val arr = line.split("\\s+")
      (arr(1), arr(2).toInt)
    }).toDF("movieId","score")
    movieDF.printSchema()
    movieDF.show()

    //计算: 电影评分次数>6, 平均分Top3

    //TODO ==================SQL===============
    movieDF.createOrReplaceTempView("t_moives")
    val sql =
      """
        |select movieId,avg(score) as avgscore,count(*) as counts
        |from t_moives
        |group by movieId
        |having counts > 6
        |order by avgscore desc
        |limit 3
      """.stripMargin
    spark.sql(sql).show()
    //TODO ==================SDL================
    import  org.apache.spark.sql.functions._
    movieDF.groupBy('movieId)
      .agg(
        avg('score ) as  "avgscore",
        count('movieId) as "counts"
    ).filter('counts > 6)
      .orderBy('avgscore.desc)
      .limit(3).show()



    //TODO 关闭资源
    spark.stop()

  }


}

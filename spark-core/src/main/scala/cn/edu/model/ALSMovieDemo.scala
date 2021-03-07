package cn.edu.model

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession

/**
  * ALS推荐算法案例
  *
  */
object ALSMovieDemo {
  def main(args: Array[String]): Unit = {
    //TODO 1.准备环境
    val spark = SparkSession.builder().appName("ALSMovieDemo").master("local[*]")
      .config("spark.sql.shuffle.partition", 4)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //TODO 2.加载数据
    val fileDS = spark.read.textFile("data/input/u.data")
    val rationDF = fileDS.map(line => {
      val arr = line.split("\t")
      (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
    }).toDF("userId", "movieId", "sorce")

    val Array(trainSet,testSet) = rationDF.randomSplit(Array(0.8,0.2))  //按照8:2 划分训练集和测试集
    //TODO 3.构建ALS算法模型并训练
    val als = new ALS().setUserCol("userId") //设置用户Id是哪一列
      .setItemCol("movieId") //设置产品id是哪一列
      .setRatingCol("sorce") //设置评分是哪一列
      .setRank(10) //可以理解为 Cm*n =Am*k  x Bk*n 里面k的值
      .setMaxIter(10) //最大迭代次数
      .setAlpha(1.0)//迭代步长

    //使用训练集训练模型
    val model:ALSModel = als.fit(trainSet)

    //使用测试集测试模型
    //val testResultDF = model.recommendForUserSubset(testSet,5)
    //计算模型误差-----模型评估
    //....

    //TODO 4.给用户做推荐
    val resultDF1= model.recommendForAllUsers(5) //给所有用户推荐5部电影
    val resultDF2 = model.recommendForAllItems(5) //给所有电影推荐5个用户
    //给196号指定用户推荐5部电影
    val resultDF3 = model.recommendForUserSubset(sc.makeRDD(Array(196)).toDF("userId"),5)
    //给242号指定电影推荐5个用户
    val resultDF4 = model.recommendForItemSubset(sc.makeRDD(Array(242)).toDF("movieId"),5)

    resultDF1.show(false)
    resultDF2.show(false)
    resultDF3.show(false)
    resultDF4.show(false)

   //todo 停止
    spark.stop()

  }
}

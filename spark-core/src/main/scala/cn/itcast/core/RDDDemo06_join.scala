package cn.itcast.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 演示Rdd的 join
  */
object RDDDemo06_join {
  def main(args: Array[String]): Unit = {
    //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //TODO 1. source/ 加载数据/ 创建RDD
    // 员工集合
    val empRDD: RDD[(Int,String)] = sc.parallelize(Seq((1001,"zhangsan"),(1002,"李四"),(1003,"wangwu")))
    // 部门集合
    val deptRDD : RDD[(Int,String)]= sc.parallelize(Seq((1101,"销售部"),(1002,"技术部"),(1004,"客服部")))

    //TODO 2. transformation
    // 内连接
    val result1: RDD[(Int,(String,String))] = empRDD.join(deptRDD)
    // 左外连接
    val result2: RDD[(Int, (String,Option[String]))] = empRDD.leftOuterJoin(deptRDD)
    // 右外连接
    val result3: RDD[(Int, (Option[String],String))] = empRDD.rightOuterJoin(deptRDD)
    //TODO 3. sink/输出/action
    result1.foreach(println)

    println(s"----------------------------------------------------")
    result2.foreach(println)

    println(s"----------------------------------------------------")
    result3.foreach(println)
  }
}

package cn.itcast.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 演示SparkSql   SQL 和 DSL两种编程方式
  *
  */
object Demo4_Query {
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

    personDataFrame.createOrReplaceGlobalTempView("person")
    //TODO  ================SQL编程=================
    //创建或提替换全局的视图  生命周期长 ,夸 sparksession 也可以用
   // personDataFrame.createOrReplaceGlobalTempView("t_person")
    //创建全局的视图 生命周期长 ,夸 sparksession 也可以用
   // personDataFrame.createGlobalTempView("t_person")

    //创建临时的视图   ,当前 sparksession  可以用
    // personDataFrame.createTempView("t_person")

    //创建提替换临时的视图   ,当前 sparksession  可以用
    personDataFrame.createOrReplaceTempView("t_person")

    //TODO  ================DSL编程=================

    // 1. 查看 name字段数据
    spark.sql("select name from t_person").show()
    // 2. 查看 name和 age 字段数据
    spark.sql("select name,age from t_person").show()
    // 3. 查询 所有的 name和 age  并将age+1
    spark.sql("select name,age+1 from t_person").show()
    // 4. 过滤age大于等于25的
    spark.sql("select name,age from t_person where age >= 25").show()
    // 5. 统计年龄大于30的人数
    spark.sql("select count(*) from t_person where age > 30").show()
    // 6. 按年龄进行分组并统计年龄相同的人数
    spark.sql("select age,count(*) from t_person group by age").show()
    // 7. 查询姓名=张三的
    spark.sql("select age,name from t_person where name = 'zhangsan'").show()


    //TODO  ================DSL编程  面向对象的sql=================
    // 1. 查看 name字段数据
    personDataFrame.select("name").show()
    // 2. 查看 name和 age 字段数据
    personDataFrame.select("name","age").show()
    // 3. 查询 所有的 name和 age  并将age+1
    //$ ` 是为了吧 列名 串转为column对象
    personDataFrame.select($"name",$"age",$"age"+1).show()
    //personDataFrame.select('name,'age,'age+1).show()
    // 4. 过滤age大于等于25的
    personDataFrame.filter($"age" >=25).show()
    // 5. 统计年龄大于30的人数
    val count = personDataFrame.where($"age"> 30).count()
    println("年龄大于30的人数: "+ count )
    // 6. 按年龄进行分组并统计年龄相同的人数
    personDataFrame.groupBy($"age").count().show()
    // 7. 查询姓名=张三的
    personDataFrame.filter($"name" ==="zhangsan").show()
    //8. 查询姓名 !=张三的
    personDataFrame.filter($"name" =!="zhangsan").show()


    //TODO 3.输出结果
    //personDataFrame.printSchema()
   // personDataFrame.show()




    //TODO 关闭资源
    spark.stop()

  }


  case class Person(id:Int,name:String,age:Int)
}

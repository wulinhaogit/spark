package cn.edu.analysis.bacth

import java.util.Properties

import cn.edu.bean.{ AnswerWithRecommendations}
import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  *  离线分析学生分析情况  从mysql 加载数据使用 sparkSQL进行离线分析
  * //需求1: 找到top50热点题对应的科目,然后统计这些科目中,分别包含这几道热点题的条目数
  *       ____.________________
  *      \科目\包含热点题数量 \
  *      \----\---------------\
  *      \数学\    98         \
  *      \----\---------------\
  *      \英语\    74         \
  *      \----\---------------\
  *      \语文\    54         \
  *
  */
object BatchAnalysis_01 {
   def main(args: Array[String]): Unit = {
      //TODO 1.主备环境
      val spark: SparkSession = SparkSession.builder().appName("BatchAnalysis").master("local[*]")
        .config("spark.sql.shuffle.partitions", "4").getOrCreate()
      val sc: SparkContext = spark.sparkContext
      sc.setLogLevel("WARN")
      import org.apache.spark.sql.functions._
      import spark.implicits._

      //TODO 2.加载数据

      val properties = new  Properties()
      properties.setProperty("user","root")
      properties.setProperty("password","root")
      val allInfoDS = spark.read.jdbc(
         "jdbc:mysql://localhost:3306/sys?useUnicode=true&characterEncoding=utf-8&useSSL=false",
         "t_recommended",
         properties
      ).as[AnswerWithRecommendations]

      //TODO 3. 处理数据

      //todo====== sql====
      //需求1: 找到top50热点题对应的科目,然后统计这些科目中,分别包含这几道热点题的条目数
         /*
         科目,包含热点题数量
          数学,98
          英语,74
          语文,54
          */
     // allInfoDS.createOrReplaceTempView("t_recommended")
     /* spark.sql(
        """
          |select
          |  t2.subject_id,count(t2.question_id )  as hot_question_count
          |from
          |    (select
          |       question_id,count(*)  as frequency
          |    from
          |       t_recommended
          |    group by
          |       question_id
          |    order by
          |       frequency desc
          |    limit 50 ) as t1
          |join
          |  t_recommended as t2
          |on
          |  t1.question_id =t2.question_id
          |group by
          |  t2.subject_id
          |order  by
          |  hot_question_count desc
        """.stripMargin).show()*/



      //todo====== sdl ===
      //需求1: 找到top50热点题对应的科目,然后统计这些科目中,分别包含这几道热点题的条目数
      //先找去 top50的热点题
      val hotTop50 = allInfoDS.groupBy($"question_id")
        .agg(count($"question_id") as "hot")
        .orderBy($"hot".desc)
        .limit(50)
      //再找top50题对应的科目和题目条数
      val result1 = hotTop50.join(allInfoDS, "question_id")
        .groupBy($"subject_id")
        .agg(count($"question_id") as "hotCount")
        .orderBy($"hotCount".desc)

      result1.show()

      //TODO 4. 输出结果

      //TODO 5. 启动并等待结束




      //TODO 6. 关闭资源
      spark.stop()
   }

}

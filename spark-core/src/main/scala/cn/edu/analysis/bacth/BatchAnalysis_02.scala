package cn.edu.analysis.bacth

import java.util.Properties

import cn.edu.bean.AnswerWithRecommendations
import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  *  离线分析学生分析情况  从mysql 加载数据使用 sparkSQL进行离线分析
  * //需求2: 找到top20热点题对应的推荐题目,然后找到推荐题目对应的推荐科目,并统计每个科目分别包含推荐题目的条数

  */
object BatchAnalysis_02 {
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
      //需求 找到top20热点题对应的推荐题目,然后找到推荐题目对应的推荐科目,并统计每个科目分别包含推荐题目的条数

      //todo====== sdl ===
      //需求1: 找到top50热点题对应的科目,然后统计这些科目中,分别包含这几道热点题的条目数
      //先找去 top20的热点题
      val hotTop50 = allInfoDS.groupBy($"question_id")
        .agg(count($"question_id") as "hot")
        .orderBy($"hot".desc)
        .limit(20)
      //再找top20题对应的推荐题
      val rids= hotTop50.join(allInfoDS.dropDuplicates("question_id"), "question_id")
          .select($"recommendations")

      //将推荐题按照,分割(split)形成新并 的展开(explode),并且去重
      val ridsDS = rids.select(explode(split($"recommendations", ",")) as "question_id")
        .dropDuplicates("question_id")
      // 将分割,展开,去重后的结果进行题目关联,并且统计个数
      val result2 = ridsDS.join(allInfoDS.dropDuplicates("question_id"), "question_id")
        .groupBy($"subject_id")
        .agg(count("*") as "rcount")
        .orderBy($"rcount".desc)


      //TODO 4. 输出结果
      result2.show()
      //TODO 5. 启动并等待结束

      //TODO 6. 关闭资源
      spark.stop()
   }

}

package cn.edu.analysis.streaming

import cn.edu.bean.Answer
import com.google.gson.Gson
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 实时的从kafka 的edu topic消费数据,并做实时统计分析,结果可以直接输出到控制台和mysql
  *
  */
object StreamingAnalysis {
   def main(args: Array[String]): Unit = {
      //TODO 1.主备环境
      val spark: SparkSession = SparkSession.builder().appName("StreamingAnalysis").master("local[*]")
        .config("spark.sql.shuffle.partitions", "4").getOrCreate()
      val sc: SparkContext = spark.sparkContext
      sc.setLogLevel("WARN")
      import spark.implicits._
      import org.apache.spark.sql.functions._

      //TODO 2.加载数据
      val kafkaDF: DataFrame = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "single:9092")
        .option("subscribe", "edu")
        .load()


      val valueDS: Dataset[String] = kafkaDF.selectExpr( "CAST(value AS STRING)").as[String]
      //{"student_id":"学生ID_32","textbook_id":"教材ID_1","grade_id":"年级ID_3","subject_id":"科目ID_3_英语","chapter_id":"章节ID_chapter_1","question_id":"题目ID_
      //495","score":5,"answer_time":"2021-02-28 17:35:31","ts":"Feb 28, 2021 5:35:31 PM"}


      //TODO 3. 处理数据
      //=========数据预处理
      //解析json
      //方式1 get_json_object

      //方式2 : 将每一条json 字符串解析为一个样例类对象(用fastJson 或者 Gson 的方式)
      val answerDS: Dataset[Answer] = valueDS.map(jsonStr => {
         val gson = new Gson()
         //json--->对象
         gson.fromJson(jsonStr, classOf[Answer])
      })
      //=========实时分析  DSL方式
      // TODO 实时分析 需求1: 统计top10热点题
      val result1: Dataset[Row] = answerDS.groupBy($"question_id")
        .count()
        .orderBy($"count".desc)
        .limit(10)

      // TODO 实时分析 需求2: 统计答题活跃年级top10
      val result2: Dataset[Row] = answerDS.groupBy($"grade_id")
        .count()
        .orderBy($"count".desc)
        .limit(10)

      // TODO 实时分析 需求3: 统计top10热点题并带上所属科目
      val result3: Dataset[Row] = answerDS.groupBy($"grade_id")
        .agg(
           first($"subject_id") as "subject_id",
           count($"question_id") as "count")
        .orderBy($"count".desc)
        .limit(10)

      // TODO 实时分析 需求4: 统计每个学生得分最低题目的top10,并带上所属题目
      val result4: Dataset[Row] = answerDS.groupBy($"student_id")
        .agg(
           min($"score") as "minscore",
           first($"subject_id") as "subject_id",
        )
        .orderBy($"minscore".desc)
        .limit(10)

      //TODO 4. 输出结果
      //TODO 5. 启动并等待结束
      result1.writeStream.format("console").outputMode(OutputMode.Complete()).start()
      result2.writeStream.format("console").outputMode(OutputMode.Complete()).start()
      result3.writeStream.format("console").outputMode(OutputMode.Complete()).start()
      result4.writeStream.format("console").outputMode(OutputMode.Complete()).start().awaitTermination()



      //TODO 6. 关闭资源
      spark.stop()
   }

}

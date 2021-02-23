package cn.itcast.core

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
;

/**
  * 演示Rdd的外部数据源-JDBC
  */
object RDDDemo11_DataSourceJDBC  {
  def main(args: Array[String]): Unit = {
    //TODO 0. env / 创建环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //TODO 1. source/ 加载数据/ 创建RDD
    //  RDD 一行行数据
    val dataRDD: RDD[(String,Int)] = sc.parallelize(Seq(("jack",18),("tom",19),("rose",20)) )

    //TODO 2. transformation


    //TODO 3. sink/输出 到mysql
    dataRDD.foreachPartition( iter => {
      // 开启连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sys?useUnicode=true&characterEncoding=utf-8&useSSL=false","root","root")

      val sql="INSERT INTO `user` (`name`,`age`) VALUES (?,?);"
      val ps = conn.prepareStatement(sql)
      iter.foreach( t => {  //t就是每一条数据
        val name = t._1
        val age = t._2
        ps.setString(1,name)
        ps.setInt(2,age)

        ps.addBatch()   //添加到批处理
       // ps.executeUpdate() //执行逐条处理
      })
      // 执行批处理
      ps.executeBatch()
      //关闭连接
      if(conn !=null)  conn.close()
      if(ps !=null)  ps.close()

    })


    // TODO 3. 从mysql 读取
    /**
      * sc: SparkContext,
      * getConnection: () => Connection,   JDBC 连接函数
      * sql: String,      sql
      * lowerBound: Long,    SQL语句中的下界
      * upperBound: Long,   SQL语句中的上界
      * numPartitions: Int,  分区数
      * mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _      结果集处理函数
      */
    val getConnection =() => DriverManager.getConnection("jdbc:mysql://localhost:3306/sys?character=UTF-8","root","root")
    val sql ="SELECT * FROM USER WHERE id >= ? AND id <= ?"
    val mapRow = (r:ResultSet) => {
        val id =r.getInt("id")
        val name =r.getString("name")
        val age =r.getInt("age")
        //return  (id,name,age)
      (id,name,age)
    }
    val userRDD = new JdbcRDD[(Int,String,Int)](sc,getConnection,sql,8,12,1,mapRow)


    userRDD.foreach(println)
    //TODO 4. 关闭资源
    sc.stop()
  }
}

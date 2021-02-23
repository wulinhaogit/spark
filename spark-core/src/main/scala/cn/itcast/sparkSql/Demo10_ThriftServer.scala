package cn.itcast.sparkSql

import java.sql.{Driver, DriverManager}

import org.apache.spark.sql.SparkSession

/**
  * 演示SparkSql :  使用jdbc访问 SparkSQL的 ThriftServer
  *
  *
  */
object Demo10_ThriftServer {
  def main(args: Array[String]): Unit = {

      //TODO  0. 加载驱动
    Class.forName("org.apache.hive.jdbc.HiveDriver")
      //TODO 1. 获取连接
    val connection = DriverManager.getConnection("jdbc:/hive2://node2:10000/default","root","123456")
      //TODO  2. 编写sql
      var sql =
        """
          |select * from person
        """.stripMargin
      //TODO  3. 获取预编译语句对象
    val ps = connection.prepareStatement(sql)
      //TODO  4. 执行sql
    val rs = ps.executeQuery()
      //TODO  5. 获取结果
    while (rs.next()){
      val id = rs.getInt("id")
      val name = rs.getString("name")
      val age = rs.getInt("age")
      println(s"id=${id},name=${name},age=${age}")
    }
      //TODO  6. 关闭资源
    if (rs!=null)  rs.close()

    if (ps!=null) ps.close()

    if (connection!=null) connection.close()




  }
}

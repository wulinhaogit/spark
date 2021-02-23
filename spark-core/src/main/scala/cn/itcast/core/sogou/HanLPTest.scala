package cn.itcast.core.sogou

import com.hankcs.hanlp.HanLP

/**
  * HanLP 分词器使用
  */
object HanLPTest {
  def main(args: Array[String]): Unit = {
    val words = "[HanLP入门案例]"
    val terms = HanLP.segment(words)

    println(terms)// 直接打印java的list :[[/w,HanLP/nx,入门/vn,案例/vn, ]/w]
    //将java 集合转换为scala集合
    import scala.collection.JavaConverters._
    println(terms.asScala.map(_.word))  //转为scala的list:   ArrayBuffer([, HanLP, 入门, 案例, ])

    val cleanWords1 = words.replaceAll("\\[|\\]","")  //将[或]替换为空
    println(cleanWords1)
    println(HanLP.segment(cleanWords1).asScala.map(_.word))//ArrayBuffer([HanLP,入门,案例])

    val log = """00:00:00 2982199073774412    [360安全卫士]    8 3 download.it.com.cn/softweb/software/firewall/antivirus/20067/17939.html"""
    val cleanWords2 = log.split("\\s+")(2) // [360安全卫士]
      .replaceAll("\\[|\\]","")//360安全卫士

    println(HanLP.segment(cleanWords2).asScala.map(_.word))//ArrayBuffer(360,安全卫士)
  }
}

package com.xiyuan.spark.task

import com.xiyuan.spark.conf.SparkConfFactory
import org.apache.spark.SparkContext

/**
  * Created by xiyuan_fengyu on 2017/11/20 19:51.
  */
object LongestLine {

  def main(args: Array[String]) {
    val conf = SparkConfFactory.fromSparkProperties().setAppName(LongestLine.getClass.getSimpleName)

    val context = new SparkContext(conf)
    val logFile = context.textFile("hdfs://192.168.1.120:9000/test.txt")
    val longestLine = logFile.map(line => (line, line.length)).max()(Ordering.by(_._2))
    println(longestLine)

    context.stop()
  }

}

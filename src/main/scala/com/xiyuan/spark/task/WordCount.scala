package com.xiyuan.spark.task

import com.xiyuan.spark.conf.SparkConfFactory
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]) {
    val conf = SparkConfFactory.fromSparkProperties().setAppName(WordCount.getClass.getSimpleName)

    val context = new SparkContext(conf)
    val logFile = context.textFile("hdfs://192.168.1.120:9000/spark-root-org.apache.spark.deploy.master.Master-1-node120.out")
    logFile.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(-_._2).collect().foreach(println)

    context.stop()
  }

}

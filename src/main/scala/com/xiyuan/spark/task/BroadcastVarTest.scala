package com.xiyuan.spark.task

import com.xiyuan.spark.conf.SparkConfFactory
import org.apache.spark.SparkContext

/**
  * Created by xiyuan_fengyu on 2017/11/22 16:47.
  */
object BroadcastVarTest {

  def main(args: Array[String]) {
    val conf = SparkConfFactory.fromSparkProperties().setAppName(BroadcastVarTest.getClass.getSimpleName)

    val context = new SparkContext(conf)
    val broadcastVar = context.broadcast(Set("one", "two", "three"))
    val rdd = context.parallelize(Array("one", "two", "three", "four"))
    rdd.filter(broadcastVar.value.contains(_)).collect().foreach(println)

    context.stop()
  }

}

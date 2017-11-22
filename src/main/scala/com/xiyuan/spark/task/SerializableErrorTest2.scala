package com.xiyuan.spark.task

import com.xiyuan.spark.conf.SparkConfFactory
import org.apache.spark.SparkContext

/**
  * Created by xiyuan_fengyu on 2017/11/22 18:06.
  */
object SerializableErrorTest2 {

  class SomethingNotSerializable () {}

  class Matcher(regex: String) extends Serializable {

    private val something = new SomethingNotSerializable

    def isMatch(str: String): Boolean = str.matches(regex)

  }

  def main(args: Array[String]) {
    val conf = SparkConfFactory.fromSparkProperties().setAppName(SerializableErrorTest1.getClass.getSimpleName)

    val context = new SparkContext(conf)
    val matcher = new Matcher("\\d+")
    context.parallelize(Array("123", "abc", "4", "5")).filter(matcher.isMatch).collect().foreach(println)

    context.stop()
  }

}
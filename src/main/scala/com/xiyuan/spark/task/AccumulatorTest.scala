package com.xiyuan.spark.task

import java.util.Scanner

import com.xiyuan.spark.conf.SparkConfFactory
import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

/**
  * Created by xiyuan_fengyu on 2017/11/22 17:06.
  */
object AccumulatorTest {

  class StrAccumulator extends AccumulatorV2[String, String] {

    protected val buffer = new StringBuffer()

    override def isZero: Boolean = buffer.length() == 0

    override def copy(): AccumulatorV2[String, String] = {
      val acc = new StrAccumulator()
      acc.buffer.append(value)
      acc
    }

    override def reset(): Unit = buffer.delete(0, buffer.length())

    override def add(v: String): Unit = buffer.append(v)

    override def merge(other: AccumulatorV2[String, String]): Unit = {
      buffer.append(other.value)
    }

    override def value: String = buffer.toString

  }

  def main(args: Array[String]) {
    val conf = SparkConfFactory.fromProperties().setAppName(AccumulatorTest.getClass.getSimpleName)
    val context = new SparkContext(conf)

    val acc = context.longAccumulator("myAcc")
    context.parallelize(Array(1, 2, 3, 4, 5)).foreach(acc.add(_))
    println(acc.value)

    val strAcc = new StrAccumulator
    context.register(strAcc, "strAcc")
    context.parallelize(Array("one", "two", "three")).foreach(strAcc.add)
    println(strAcc.value)

    println("press enter to exit")
    new Scanner(System.in).nextLine()
    context.stop()
  }

}


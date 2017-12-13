package com.xiyuan.spark.streaming

import com.xiyuan.spark.conf.SparkConfFactory
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by xiyuan_fengyu on 2017/11/27 16:25.
  */
object StreamingFromFlume {

  def main(args: Array[String]) {
    val conf = SparkConfFactory.fromProperties("properties/spark.flume.properties")
    val context = new StreamingContext(conf, Seconds(5))
    val flumeStream = FlumeUtils.createPollingStream(context, "192.168.1.220", 9090)

    flumeStream.count().map("received " + _ + " flume events").print()

    context.start()
    context.awaitTermination()
  }

}
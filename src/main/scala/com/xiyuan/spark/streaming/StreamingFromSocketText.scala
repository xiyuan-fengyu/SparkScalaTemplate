package com.xiyuan.spark.streaming

import com.google.gson.{Gson, JsonObject}
import com.xiyuan.spark.conf.SparkConfFactory
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by xiyuan_fengyu on 2017/11/27 10:25.
  * 先运行 com.xiyuan.spark.streaming.SocketServer 启动socket服务器
  */
object StreamingFromSocketText {

  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "root")

    /*
    目前比较好的时间配置

    new StreamingContext(conf, Seconds(3))
    window(Seconds(30), Seconds(6))
    最新数据延迟在5秒左右，执行间隔在6秒左右，刚启动时就可以稳定运行

    new StreamingContext(conf, Seconds(2))
    window(Seconds(20), Seconds(4))
    最新数据延迟在4秒左右，执行间隔在5秒左右，刚启动时不稳定，延迟较高，运行一分钟左右开始稳定

    window + reduceByKey 的效率在实际运行中表现比 reduceByKeyAndWindow 更好
     */

    val conf = SparkConfFactory.fromProperties("properties/spark.kafka.properties")
    val context = new StreamingContext(conf, Seconds(2))
//    val checkpointDir = "hdfs://192.168.1.120:9000/checkpoint/StreamingFromKafka"
//    context.checkpoint(checkpointDir)

    val stream = context.socketTextStream("192.168.1.66", 9999)

    val ipCounts = new mutable.HashMap[Long, Array[(String, (Int, String))]]()
    val ipCountsBrd = context.sparkContext.broadcast(ipCounts)

    stream
      .map(item => {
        val split = item.split(" ")
        (split(0), (1, split(2)))
      })
      .window(Seconds(20), Seconds(4))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, if (v1._2.compareTo(v2._2) > 0) v1._2 else v2._2))
      .filter(_._2._1 >= 5)
      .foreachRDD(rdd => {
        val now = System.currentTimeMillis()
        if (!ipCountsBrd.value.contains(now)) {
          val temp = rdd.collect().sortBy(-_._2._1)
          //        logger.info(s"time=$now\trddCount=${rdd.count()}")
          //        logger.info(rdd + ": " + temp.map(item => item.toString()).reduce(_ + ", " + _))
          ipCountsBrd.value += now -> temp
        }
      })

    //通过 websocket 将数据实时展示在网页上
    val webUI = new WebUI(9000)
    new Thread() {
      override def run(): Unit = {
        val gson = new Gson
        while (!context.sparkContext.isStopped) {
          val now = System.currentTimeMillis()
          if (ipCountsBrd.value.nonEmpty) {
            val last = ipCountsBrd.value.keys.max
            val lastData = ipCountsBrd.value(last)
            ipCountsBrd.value.keysIterator.foreach(key => {
              if (key <= last) {
                ipCountsBrd.value.remove(key)
              }
            })
            val msg = new JsonObject
            msg.addProperty("id", System.currentTimeMillis() + "_" + (Math.random() * 1000).toInt)
            msg.addProperty("key", "dataPush")
            msg.add("data", gson.toJsonTree(lastData))
            webUI.broadcast(msg)
          }
          Thread.sleep(1000)
        }
        webUI.stop()
        println("WebUI stoped")
      }
    }.start()


    context.start()
    context.awaitTermination()
  }

}
package com.xiyuan.spark.streaming

import com.google.gson.{Gson, JsonObject}
import com.xiyuan.spark.conf.{ConsumerStrategiesFactory, SparkConfFactory}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * Created by xiyuan_fengyu on 2017/11/27 16:25.
  * 官方参考文档
  * http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
  *
  * 增量方式
  * https://www.csdn.net/article/2014-01-28/2818282-Spark-Streaming-big-data
  * http://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing
  * 如何使用checkpoint
  * http://www.jianshu.com/p/00b591c5f623
  * 权限问题参考
  * http://blog.csdn.net/lunhuishizhe/article/details/50489849
  *
  * 先启动本程序，再运行 com.xiyuan.spark.streaming.MyKafkaProducer 启动 kafka producer
  */
object StreamingFromKafka {

  private val logger = LoggerFactory.getLogger("xy")

  def main(args: Array[String]) {
    //解决权限问题，用户名根据具体报错确定
    //Exception in thread "main" org.apache.hadoop.security.AccessControlException: Permission denied: user=hadoop, access=WRITE, inode="/":root:supergroup:drwxr-xr-x
    System.setProperty("HADOOP_USER_NAME", "root")

    val checkpointDir = "hdfs://192.168.1.120:9000/checkpoint/StreamingFromKafka"
//    val context = StreamingContext.getOrCreate(checkpointDir, () => {
//      val conf = SparkConfFactory.fromProperties("properties/spark.kafka.properties")
//      val newContext = new StreamingContext(conf, Seconds(1))
//      newContext.checkpoint(checkpointDir)
//      newContext
//    })

    val conf = SparkConfFactory.fromProperties("properties/spark.kafka.properties")
    val context = new StreamingContext(conf, Seconds(1))
    context.checkpoint(checkpointDir)
    context.remember(Seconds(60))

    val stream = KafkaUtils.createDirectStream[String, String](
      context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategiesFactory.fromProperties[String, String]()
    )

    val ipCounts = new mutable.HashMap[Long, Array[(String, (Int, String))]]()
    val ipCountsBrd = context.sparkContext.broadcast(ipCounts)

    stream.map(item => {
      val split = item.value().split(" ")
      (split(0), (1, split(2)))
    })
      .reduceByKeyAndWindow(
        (v1: (Int, String), v2: (Int, String)) => (v1._1 + v2._1, if (v1._2.compareTo(v2._2) > 0) v1._2 else v2._2)
        , Seconds(30), Seconds(10))
      .foreachRDD(rdd => {
        val now = System.currentTimeMillis()
        if (!ipCountsBrd.value.contains(now)) {
          val temp = rdd.top(50).sortBy(-_._2._1)
          //        logger.info(s"time=$now\trddCount=${rdd.count()}")
          //        logger.info(rdd + ": " + temp.map(item => item.toString()).reduce(_ + ", " + _))
          ipCountsBrd.value += now -> temp
        }
      })

//    //每隔一秒打印访问频率最高的 ip 地址
//    new Thread() {
//      override def run(): Unit = {
//        while (true) {
//          if (ipCountsBrd.value.nonEmpty) {
//            println(ipCountsBrd.value(0))
//          }
//          Thread.sleep(1000)
//        }
//      }
//    }.start()

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
package com.xiyuan.spark.streaming

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.xiyuan.spark.conf.ConsumerStrategiesFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import com.xiyuan.spark.streaming.RandomUtil._

/**
  * Created by xiyuan_fengyu on 2017/11/27 16:59.
  * 模拟发送 nginx 日志
  */
object MyKafkaProducer {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.load(ConsumerStrategiesFactory.getClass.getClassLoader.getResourceAsStream("properties/kafka.producer.properties"))
    val topic = properties.getProperty("topic")
    val producer = new KafkaProducer[String, String](properties)

    val randomIps = new ArrayBuffer[String]()
    for (i <- 0 until 10) {
      randomIps += randomIp()
    }

    //程序模拟发送
    val format = new SimpleDateFormat("yyyy_MM_dd__HH_mm_ss_SSS")
    var index = 0
    while (index < Long.MaxValue) {
      val accessLog = randomIps(randomInt(0, randomIps.length - 1)) + " " + randomPath(randomInt(-1, 3)) + " " + format.format(new Date)
      println(accessLog)
      producer.send(new ProducerRecord[String, String](topic, accessLog))
      index += 1
      Thread.sleep(30)
    }
    producer.close()
  }

}

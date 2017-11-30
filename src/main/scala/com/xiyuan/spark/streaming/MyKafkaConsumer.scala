package com.xiyuan.spark.streaming

import java.util.{Collections, Properties}

import com.xiyuan.spark.conf.ConsumerStrategiesFactory
import org.apache.kafka.clients.consumer.KafkaConsumer

/**
  * Created by xiyuan_fengyu on 2017/11/27 16:59.
  */
object MyKafkaConsumer {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.load(ConsumerStrategiesFactory.getClass.getClassLoader.getResourceAsStream("properties/kafka.consumer.properties"))
    val topic = properties.getProperty("topic")
    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(Collections.singleton(topic))
    var isRunning = true
    while (isRunning) {
      try {
        val it = consumer.poll(5000).iterator()
        while (it.hasNext) {
          val cur = it.next()
          println(cur.value())
        }
      }
      catch {
        case e: Exception =>
          isRunning = false
      }
    }
    consumer.close()
  }

}

package com.xiyuan.spark.conf

import java.util.Properties

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiyuan_fengyu on 2017/11/27 16:42.
  */
object ConsumerStrategiesFactory {

  def fromProperties[T1, T2](resource: String = "properties/kafka.streaming.properties"): ConsumerStrategy[T1, T2] = {
    val kafkaCfg = mutable.HashMap[String, Object]()
    val topics = ArrayBuffer[String]()
    val in = ConsumerStrategiesFactory.getClass.getClassLoader.getResourceAsStream(resource)
    if (in != null) {
      val properties = new Properties()
      properties.load(in)
      properties.stringPropertyNames().foreach(key => {
        if (key == "topics") {
          topics ++= properties.getProperty(key).split(",").map(_.trim)
        }
        else {
          kafkaCfg.put(key, properties.getProperty(key))
        }
      })
    }
    ConsumerStrategies.Subscribe[T1, T2](topics.toArray, kafkaCfg)
  }

}

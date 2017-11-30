package com.xiyuan.spark.conf

import java.io.File
import java.util.Properties

import org.apache.spark.SparkConf

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object SparkConfFactory {

  private def findJarFromPaths(paths: String): Array[String] = {
    val buffer = new ArrayBuffer[String]()
    if (paths != null && !paths.isEmpty) {
      paths.split(";").foreach(path => findJarFromFile(new File(path), buffer))
    }
    buffer.toArray
  }

  private def findJarFromFile(file: File, buffer: ArrayBuffer[String]): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        val subs = file.listFiles()
        if (subs != null) {
          subs.foreach(file => {
            findJarFromFile(file, buffer)
          })
        }
      }
      else if (file.getCanonicalPath.endsWith(".jar")) {
        buffer += "file:/" + file.getCanonicalPath.replace('\\', '/')
      }
    }
  }

  def fromProperties(resource: String = "properties/spark.properties"): SparkConf = {
    val sparkConf = new SparkConf()
    val in = SparkConfFactory.getClass.getClassLoader.getResourceAsStream(resource)
    if (in != null) {
      val properties = new Properties()
      properties.load(in)
      properties.stringPropertyNames().foreach(key => {
        if (key == "spark.jars") {
          sparkConf.setJars(findJarFromPaths(properties.getProperty(key)))
        }
        else {
          sparkConf.set(key, properties.getProperty(key))
        }
      })
    }
    sparkConf
  }

}

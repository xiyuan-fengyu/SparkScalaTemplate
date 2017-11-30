package com.xiyuan.spark.streaming

/**
  * Created by xiyuan_fengyu on 2017/11/29 12:16.
  */
object RandomUtil {

  def randomInt(min: Int, max: Int): Int = {
    (Math.random() * (max - min + 1)).toInt + min
  }

  def randomIp(): String = {
    randomInt(1, 254) + "." + randomInt(1, 254) + "." + randomInt(1, 254) + "." + randomInt(1, 254)
  }

  def randomPath(depth: Int): String = {
    val builder = new StringBuilder
    if (depth == -1) {
      builder.append("/")
    }
    else {
      for (i <- 0 to depth) {
        builder.append("/")
        builder.append(randomInt('a', 'c').toChar)
      }
    }
    builder.toString()
  }

}

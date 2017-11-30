package com.xiyuan.spark.streaming

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.CountDownLatch

import com.xiyuan.spark.streaming.RandomUtil.{randomInt, randomIp, randomPath}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelHandlerAdapter, ChannelHandlerContext, ChannelInitializer}
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by xiyuan_fengyu on 2017/11/27 11:05.
  */
object SocketServer {

  def main(args: Array[String]): Unit = {
    val server = new SocketServer(9999)
    server.waitForClient()

    //手动输入
//    val it = Source.fromInputStream(System.in, "utf-8").getLines()
//    while (it.hasNext && server.isRunning) {
//      val line = it.next
//      if (line == "exit") {
//        server.stop()
//      }
//      else {
//        server.broadcast(line)
//      }
//    }

    val randomIps = new ArrayBuffer[String]()
    for (i <- 0 until 50) {
      randomIps += randomIp()
    }


//    val deque = new util.ArrayDeque[(String, Long)]()
//    new Thread() {
//      override def run(): Unit = {
//        while (isAlive) {
//          val now = System.currentTimeMillis()
//          while (!deque.isEmpty && now - deque.peek()._2 > 30000) {
//            deque.poll()
//          }
//
//          val map = new mutable.HashMap[String, Int]()
//          deque.foreach(item => {
//            val old = map.getOrElse(item._1, 0)
//            map.put(item._1, old + 1)
//          })
//
//          println(map.maxBy(_._2))
//
//          Thread.sleep(1000)
//        }
//      }
//    }.start()

    //程序模拟发送
    var index = 0
    val format = new SimpleDateFormat("yyyy_MM_dd__HH_mm_ss_SSS")
    while (index < Long.MaxValue) {


      val ip = randomIps(randomInt(0, randomIps.length - 1))
      val accessLog = ip + " " + randomPath(randomInt(-1, 3)) + " " + format.format(new Date)
//      deque.add((ip, System.currentTimeMillis()))
      println(accessLog)
      server.broadcast(accessLog)
      index += 1
      Thread.sleep(30)
    }

    server.stop()
  }

}

class SocketServer(port: Int) {

  private val clients = new mutable.HashSet[ChannelHandlerContext]

  private val countDownLatch = new CountDownLatch(1)

  private var running = false

  private val (bossGroup, workerGroup) =
  {
    running = true
    val bossGroup = new NioEventLoopGroup()
    val workerGroup = new NioEventLoopGroup()
    try {
      val server = new ServerBootstrap()
      server
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new MyChannelHandler)
        .bind(port).sync().channel().closeFuture()
    }
    catch {
      case _: Exception =>
        stop()
    }
    (bossGroup, workerGroup)
  }

  def isRunning: Boolean = running

  def waitForClient(): Unit = {
    if (clients.isEmpty) {
      countDownLatch.await()
    }
  }

  def stop(): Unit = {
    running = false
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
  }

  def broadcast(msg: String): Unit = {
    clients.foreach(client => {
      client.channel().writeAndFlush(msg + "\n")
    })
  }

  private class MyChannelHandler extends ChannelInitializer[SocketChannel] {
    override def initChannel(c: SocketChannel): Unit = {
      val pipeline = c.pipeline()
      pipeline.addLast(new LineBasedFrameDecoder(65535))
      pipeline.addLast(new StringDecoder())
      pipeline.addLast(new StringEncoder())
      pipeline.addLast(new MyChannelAdapter())
    }
  }

  private class MyChannelAdapter extends ChannelHandlerAdapter {

    override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
      clients.add(ctx)
      countDownLatch.countDown()
      println("handlerAdded: " + ctx)
    }

    override def handlerRemoved(ctx: ChannelHandlerContext): Unit = {
      clients.remove(ctx)
      println("handlerRemoved: " + ctx)
    }

  }

}
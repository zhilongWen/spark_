package com.at.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zero
 * @create 2021-03-22 19:12
 */
object SparkStreaming01_WordCount {

  def main(args: Array[String]): Unit = {

    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999, StorageLevel.DISK_ONLY)


    val flatMapDS: DStream[String] = socketDS.flatMap(_.split(" "))

    val res: DStream[(String, Int)] = flatMapDS.map((_, 1)).reduceByKey(_ + _)

    res.print()


    //启动采集器
    ssc.start()

    //默认情况下，采集器不能关闭
    //ssc.stop()

    //等待采集器结束后，终止程序
    ssc.awaitTermination()


  }


}

package com.at.spark.day11

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
  * Author: Felix
  * Date: 2020/5/20
  * Desc: 优雅的关闭思路，使用标记位
  * https://blog.csdn.net/zwgdft/article/details/85849153
  */
object SparkStreaming04_gracefulStop {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //设置优雅的关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9991)

    //扁平化
    val flatMapDS: DStream[String] = socketDS.flatMap(_.split(" "))
    
    //结构转换  进行计数
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_,1))

    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_+_)

    //打印输出
    reduceDS.print

    // 启动新的线程，希望在特殊的场合关闭SparkStreaming
    new Thread(new Runnable {
      override def run(): Unit = {

        while ( true ) {
          try {
            Thread.sleep(5000)
          } catch {
            case ex : Exception => println(ex)
          }

          // 监控HDFS文件的变化
          val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"), new Configuration(), "zero")

          val state: StreamingContextState = ssc.getState()
          // 如果环境对象处于活动状态，可以进行关闭操作
          if ( state == StreamingContextState.ACTIVE ) {

            // 判断路径是否存在
            val flg: Boolean = fs.exists(new Path("hdfs://hadoop102:8020/stopSpark"))
            if ( flg ) {
              ssc.stop(true, true)
              System.exit(0)
            }

          }
        }

      }
    }).start()




    //启动采集器
    ssc.start()

    //默认情况下，采集器不能关闭
    //ssc.stop()


    //等待采集结束之后，终止程序
    ssc.awaitTermination()
  }
}

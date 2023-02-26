package com.at.spark.day11

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * @author zero
 * @create 2021-03-23 13:50
 */
object SparkStreaming01_Transform {

  /*

    使用transform算子将DS转换为rdd


   */

  def main(args: Array[String]): Unit = {

    //创建配置文件对象   注意：Streaming程序执行至少需要2个线程，所以不能设置为local
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    //创建SparkStreaming程序执行入口对象（上下文环境对象）
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //从指定的端口获取数据
    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9991)


    //socketDS.sortByKey

    socketDS.transform(
      rdd => {
        rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortByKey()
      }
    ).print()



    ssc.start()
    ssc.awaitTermination()



  }
}

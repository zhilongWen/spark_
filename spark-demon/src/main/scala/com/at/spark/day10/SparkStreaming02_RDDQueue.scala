package com.at.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author zero
 * @create 2021-03-22 19:27
 */
object SparkStreaming02_RDDQueue {

  /*
      通过RDD队列方式创建DStream

      循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，计算WordCount
   */
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    val rddQueue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()

    val queueDS: InputDStream[Int] = ssc.queueStream(rddQueue,false)

    //处理采集到的数据
    val resDS: DStream[(Int, Int)] = queueDS.map((_,1)).reduceByKey(_+_)

    //打印结果
    resDS.print()


    ssc.start()

    //循环创建RDD，并将创建的RDD放到队列里
    for (elem <- 1 to 5) {
      rddQueue.enqueue(ssc.sparkContext.makeRDD(6 to 10))
      Thread.sleep(2000)
    }



    ssc.awaitTermination()

  }

}

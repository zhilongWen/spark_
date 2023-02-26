package com.at.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zero
 * @create 2021-03-22 21:01
 */
object SparkStreaming04_ReceiverAPI {

  /*

      通过ReceiverAPI连接Kafka数据源，获取数据

   */

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    //连接Kafka，创建DStream
    val kafkaDS: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "bigdata",
      Map("bigdata001" -> 2)
    )


    val res: DStream[(String, Int)] = kafkaDS.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    res.print()


    ssc.start()
    ssc.awaitTermination()



  }

}

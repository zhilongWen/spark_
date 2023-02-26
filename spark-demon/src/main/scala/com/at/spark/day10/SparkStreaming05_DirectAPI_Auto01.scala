package com.at.spark.day10

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @author zero
 * @create 2021-03-22 21:26
 */
object SparkStreaming05_DirectAPI_Auto01 {

  /*
      通过DirectAPI连接Kafka数据源，获取数据

        自定的维护偏移量，偏移量维护在checkpiont中
        目前我们这个版本，只是指定的检查点，只会将offset放到检查点中，但是并没有从检查点中取，会存在消息丢失

   */

  def main(args: Array[String]): Unit = {

    var conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    //设置检查点目录
    ssc.checkpoint("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\cp")

    //准备Kafka参数
    val kafkaParams: Map[String, String] = Map[String, String](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        ConsumerConfig.GROUP_ID_CONFIG -> "bigdata"
    )

    //连接Kafka，创建DStream
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set("bigdata001")
    )

    val res: DStream[(String, Int)] = kafkaDS.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    res.print()


    ssc.start()
    ssc.awaitTermination()



  }

}

package com.at.spark.day10

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zero
 * @create 2021-03-22 21:26
 */
object SparkStreaming07_DirectAPI_Hander {

  /*


   */

  def main(args: Array[String]): Unit = {

    var conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    //准备Kafka参数
    val kafkaParams: Map[String, String] = Map[String, String](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        ConsumerConfig.GROUP_ID_CONFIG -> "bigdata"
    )

    //获取上一次消费的位置（偏移量）
    //实际项目中，为了保证数据精准一致性，我们对数据进行消费处理之后，将偏移量保存在有事务的存储中， 如MySQL
    var fromOffsets: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long](
      TopicAndPartition("bigdata001",0) -> 0L,
      TopicAndPartition("bigdata001",1) -> 0L
    )


    //连接Kafka，创建DStream
    val kafkaDS: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaParams,
      fromOffsets,
      (m: MessageAndMetadata[String, String]) => m.message()
    )


    //消费完毕之后，对偏移量offset进行更新
    var offsetRanges = Array.empty[OffsetRange]

    kafkaDS.transform {
      rdd =>{
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }



    ssc.start()
    ssc.awaitTermination()



  }

}

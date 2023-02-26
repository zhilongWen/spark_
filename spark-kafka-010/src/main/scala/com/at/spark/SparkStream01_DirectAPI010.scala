package com.at.spark



import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.language.postfixOps

/**
 * @author zero
 * @create 2021-03-23 13:26
 */
object SparkStream01_DirectAPI010 {

  /*

    通过DirectAPI 0-10消费Kafka数据

    消费的offset保存在__consumer_offsets主题中

   */

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("bigdata001"), kafkaParams)
    )

    kafkaDS.map(_.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()




    ssc.start()
    ssc.awaitTermination()



  }

}

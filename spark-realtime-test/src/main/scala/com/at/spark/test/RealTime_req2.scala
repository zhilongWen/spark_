package com.at.spark.test

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date


object RealTime_req2 {

  /*

    最近1小时广告点击量实时统计
      统计各广告最近1小时内的点击量趋势，每6s更新一次（各广告最近1小时内各分钟的点击量）

      -采集周期： 3s
      -最近一小时:  窗口的长度为1小时
      -每6s更新一次：窗口滑动的步长
      -各分钟的点击量   ((advId,hhmm),1)

   */

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")

    val ssc = new StreamingContext(conf,Seconds(3) )

    ssc.checkpoint("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-realtime-test\\cp")

    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "my-ads-001"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    //创建DS
    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic))


    //msg = 1616492791461,华东,上海,103,1
    val dataDS: DStream[String] = kafkaDS.map(_._2)


    //定义窗口大小以及滑动的步长
    val winDS: DStream[String] = dataDS.window(Seconds(12), Seconds(3))

    //对结构进行转换 (advId_hhmm,1)
    val res: DStream[(String, Int)] = winDS.map(
      line => {
        val fileds: Array[String] = line.split(",")

        val date: String = new SimpleDateFormat("mm:ss").format(new Date(fileds(0).toLong))

        (date + "_" + fileds(4), 1)
      }
    ).reduceByKey(_ + _)


    res.print()


    ssc.start()
    ssc.awaitTermination()
  }
}

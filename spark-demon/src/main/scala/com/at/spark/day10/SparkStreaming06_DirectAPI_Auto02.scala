package com.at.spark.day10

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zero
 * @create 2021-03-22 21:26
 */
object SparkStreaming06_DirectAPI_Auto02 {

  /*
      通过DirectAPI连接Kafka数据源，获取数据

        自定的维护偏移量，偏移量维护在checkpiont中

        修改StreamingContext对象的获取方式，先从检查点获取，如果检查点没有，通过函数创建。会保证数据不丢失

        缺点：
          1.小文件过多
          2.在checkpoint中，只记录最后offset的时间戳，再次启动程序的时候，会从这个时间到当前时间，把所有周期都执行一次


   */

  def main(args: Array[String]): Unit = {


    val ssc: StreamingContext = StreamingContext.getActiveOrCreate(
      "D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\cp",
      () => getStreamingContext
    )

    ssc.start()
    ssc.awaitTermination()

  }

  def getStreamingContext():StreamingContext ={
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

    ssc

  }

}





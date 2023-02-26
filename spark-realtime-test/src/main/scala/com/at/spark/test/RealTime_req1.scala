package com.at.spark.test

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date


object RealTime_req1 {

  /*

      	每天每地区热门广告Top3

   */

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf,Milliseconds(500) )

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


    //msg = 1616492791461,华东,上海,103,1 => (1616492791461_华东上海_10,1)
    val mapDS: DStream[(String, Long)] = kafkaDS.map(_._2).map(
      datas => {

        val fields: Array[String] = datas.split(",")

        val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(fields(0).toLong))

        val area: String = fields(1)

        val adv: String = fields(4)


        (date + "_" + area + "_" + adv, 1L)

      }
    )

    //对每天每地区广告点击数进行聚合处理   (天_地区_广告,sum)
    //注意：这里要统计的是一天的数据，所以要将每一个采集周期的数据都统计，需要传递状态，所以要用udpateStateByKey
//    (1616492791461_华东上海_10,19999)
    val updateDS: DStream[(String, Long)] = mapDS.updateStateByKey(
      (seq: Seq[Long], buffer: Option[Long]) => {
        Option(seq.sum + buffer.getOrElse(0L))
      }
    )

    //将相同的天和地区放到一组
    //(天_地区,(广告,sum))
    val groupDS: DStream[(String, Iterable[(String, Long)])] = updateDS.map {
      case (k, sum) => {
        val fileds: Array[String] = k.split("_")
        (fileds(0) + "_" + fileds(1), (fileds(2), sum))
      }
    }.groupByKey()

    val res: DStream[(String, List[(String, Long)])] = groupDS.mapValues(
      datas => {
        datas.toList.sortBy(-_._2).take(3)
      }
    )



    res.print()





    ssc.start()
    ssc.awaitTermination()
  }
}

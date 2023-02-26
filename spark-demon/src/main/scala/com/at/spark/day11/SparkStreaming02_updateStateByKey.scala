package com.at.spark.day11

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zero
 * @create 2021-03-23 13:59
 */
object SparkStreaming02_updateStateByKey {


  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    ssc.checkpoint("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\cp")

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9991)

    val mapRDD: DStream[(String, Int)] = socketDS.flatMap(_.split(" ")).map((_, 1))

    //聚合   reduceByKey是无状态的，只会对当前采集周期的数据进行聚合操作
//    mapRDD.reduceByKey(_+_).print()

    mapRDD.updateStateByKey(
      //第一个参数：表示的相同的key对应的value组成的数据集合
      //第二个参数：表示的相同的key的缓冲区数据
      (seq:Seq[Int],buffer:Option[Int]) => {
        //对当前key对应的value进行求和
        //seq.sum
        //获取缓冲区数据
        //state.getOrElse(0)
        Option(seq.sum + buffer.getOrElse(0))
      }
    ).print()


    ssc.start()
    ssc.awaitTermination()

  }

}

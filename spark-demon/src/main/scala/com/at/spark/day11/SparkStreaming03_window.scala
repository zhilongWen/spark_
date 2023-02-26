package com.at.spark.day11

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zero
 * @create 2021-03-23 14:14
 */
object SparkStreaming03_window {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9991)

    socketDS.window()

    //设置窗口的大小以及滑动步长   以上两个值都应该是采集周期的整数倍
    val winDS: DStream[String] = socketDS.window(
      Seconds(6),
      Seconds(3)
    )

    val res: DStream[(String, Int)] = winDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

//    res.print()

    res.saveAsTextFiles("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\output\\cpcp","opop")


    /*
    res.foreachRDD(
    rdd=>{
      //将RDD中的数据保存到MySQL数据库
      rdd.foreachPartition{
        //注册驱动
        //获取连接
        //创建数据库操作对象
        //执行SQL语句
        //处理结果集
        //释放资源
        datas=>{
            //....
        }
      }
    }
  )
  */


    //在DStream中使用累加器，广播变量以及缓存
    //ssc.sparkContext.longAccumulator
    //ssc.sparkContext.broadcast(10)

    //缓存
    //resDS.cache()
    //resDS.persist(StorageLevel.MEMORY_ONLY)

    //检查点
    //ssc.checkpoint("")


    //使用SparkSQL处理采集周期中的数据
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    res.foreachRDD(
      rdd=>{
        rdd
        //将RDD转换为DataFrame
        val df: DataFrame = rdd.toDF("word","count")
        //创建一个临时视图
        df.createOrReplaceTempView("words")
        //执行SQL
        spark.sql("select * from words").show
      }
    )




    ssc.start()
    ssc.awaitTermination()

  }

}

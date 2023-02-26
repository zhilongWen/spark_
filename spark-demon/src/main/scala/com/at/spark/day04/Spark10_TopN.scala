package com.at.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-15 22:05
 */
object Spark10_TopN {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val textRDD: RDD[String] = sc.textFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input\\agent.log")

    val vals: RDD[((String, String), Int)] = textRDD.filter(_.split(" ").size == 5).map(datas => {
      val arr: Array[String] = datas.split(" ")
      ((arr(1), arr(4)), 1)
    })

    val reduceRDD: RDD[((String, String), Int)] = vals.reduceByKey(_ + _)

    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((city, gg), num) => {
        (city, (gg, num))
      }
    }

    val groupRDD: RDD[(String, Iterable[(String, (String, Int))])] = mapRDD.groupBy(_._1)

    val value: RDD[(String, List[(String, Int)])] = groupRDD.map {
      case (str, iter) => {
        (str, iter.toList.map {
          case (str, kv) => (kv._1, kv._2)
        })
      }
    }

    val value1: RDD[(String, List[(String, Int)])] = value.mapValues(list => {
      list.sortBy(_._2).take(3)
    })

    value1.collect().foreach(println)


    // 关闭连接
    sc.stop()


    /*

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //1.读取外部文件，创建RDD    时间戳 省份id 城市id 用户id 广告id
    val logRDD: RDD[String] =sc.textFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input\\agent.log")

    //2.对读取到的数据，进行结构转换  (省份id-广告id,1)
    val mapRDD: RDD[(String, Int)] = logRDD.map {
      line => {
        //2.1  用空格对读取的一行字符串进行切分
        val fields: Array[String] = line.split(" ")
        //2.2 封装为元组结构返回
        (fields(1) + "-" + fields(4), 1)
      }
    }

    //3.对当前省份的每一个广告点击次数进行聚合  (省份A-广告A,1000)  (省份A-广告B,800)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey( _ + _ )

    //4.再次对结构进行转换，将省份作为key   (省份A,(广告A,1000))  (省份A,(广告B,800))
    val map1RDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case (proAndAd, clickCount) => {
        val proAndAdArr: Array[String] = proAndAd.split("-")
        (proAndAdArr(0), (proAndAdArr(1), clickCount))
      }
    }

    //5.按照省份对数据进行分组    (省份, Iterable[(广告A, 80),(广告B, 100),(广告C, 90),(广告D, 200)....])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = map1RDD.groupByKey()

    //6.对每一个省份中的广告点击次数进行降序排序并取前3名
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      itr => {
        //itr.toList.sortBy(_._2).reverse.take(3)
        itr.toList.sortWith {
          (left, right) => {
            left._2 > right._2
          }
        }.take(3)
      }
    )

    groupRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()

*/






  }

}

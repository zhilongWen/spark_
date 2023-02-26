package com.at.spark

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author zero
 * @create 2021-03-17 19:49
 */
package object day07 {



  def getActionRDD(sc: SparkContext):RDD[UserVisitAction]={

    //读取数据
    val textRDD: RDD[String] = sc.textFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input\\user_visit_action.txt")

    //封装数据
    val actionRDD: RDD[UserVisitAction] = textRDD.map(
      line => {
        val fields: Array[String] = line.split("_")
        UserVisitAction(
          fields(0),
          fields(1).toLong,
          fields(2),
          fields(3).toLong,
          fields(4),
          fields(5),
          fields(6).toLong,
          fields(7).toLong,
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toLong
        )
      }
    )

    actionRDD

  }



}

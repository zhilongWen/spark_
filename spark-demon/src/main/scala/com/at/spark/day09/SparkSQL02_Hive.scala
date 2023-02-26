package com.at.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author zero
 * @create 2021-03-21 22:26
 */
object SparkSQL02_Hive {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()


    spark.sql("show tables").show()


    spark.stop()

  }

}

package com.at.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author zero
 * @create 2021-03-21 14:41
 */
object SparkSQL02_UDF {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input\\test.json")

    //注册自定义函数
    spark.udf.register("sayHI",(name:String) => {"nihao:"+name})

    df.createOrReplaceTempView("user")

    spark.sql("select sayHI(username) as newname,age from user").show()

    spark.stop()


  }

}

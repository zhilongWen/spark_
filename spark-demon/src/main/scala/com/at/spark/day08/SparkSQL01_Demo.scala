package com.at.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author zero
 * @create 2021-03-21 13:40
 */
object SparkSQL01_Demo {


  def main(args: Array[String]): Unit = {


    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL").setMaster("local[*]")

    //创建SparkSQL执行的入口点对象  SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取json文件创建DataFrame
//    val df: DataFrame = spark.read.json("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input\\test.json")


    import spark.implicits._

    //查看df里面的数据
//    df.show()

    //SQL语法风格
//    df.createOrReplaceTempView("user")
//    df.createGlobalTempView("user")
//    spark.sql("select * from global_temp.user").show()

    //DSL风格
//    df.select("username","age").show()



    //RDD--->DataFrame--->DataSet
    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "banzhang", 20), (2, "jingjing", 18), (3, "qiangqiang", 30)))


    //RDD--->DataFrame
    val df: DataFrame = rdd.toDF("id", "name", "age")
//    df.show()


    //DataFrame--->DataSet
    val ds: Dataset[User] = df.as[User]
//    ds.show()

    //DataSet--->DataFrame--->RDD
    val df1: DataFrame = ds.toDF()

    val rdd1: RDD[Row] = df1.rdd

    rdd1.foreach(println)

    spark.stop()


  }

}


case class User(id:Int,name:String,age:Int)
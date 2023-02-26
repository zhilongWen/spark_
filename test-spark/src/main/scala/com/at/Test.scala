package com.at

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @create 2021-08-07 
 */
object Test {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate();


    val ch: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("Tom", 79), ("Lily", 84), ("Sopia", 99), ("Jerry", 80), ("Chery", 67)))
    val ma: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("Tom", 79), ("Lily", 84), ("Sopia", 90), ("Jim", 67), ("Jack", 77)))


    import spark.implicits._

    val chFrame = ch.toDF("name","ch_core")
    val maFrame = ma.toDF("name","ma_core")


//    chFrame.show()


    chFrame.createTempView("tb_ch")
    maFrame.createTempView("tb_ma")

    spark.sql("select * from tb_ch").show()

    println("----------------------------------------------------")

    spark.sql("select * from tb_ma").show()

    println("----------------------- join  ----------------------------")
    spark.sql(
      """
        |select
        |    ch.name,
        |    ch.ch_core,
        |    ma.ma_core
        |from
        |    tb_ch ch
        |join tb_ma ma
        |on ch.name=ma.name
        |""".stripMargin).show()

    println("----------------------- left join  ----------------------------")
    spark.sql(
      """
        |select
        |    ch.name,
        |    ch.ch_core,
        |    ma.ma_core
        |from
        |    tb_ch ch
        |left join tb_ma ma
        |on ch.name=ma.name
        |""".stripMargin).show()

    println("----------------------- left outer join  ----------------------------")
    spark.sql(
      """
        |select
        |    ch.name,
        |    ch.ch_core,
        |    ma.ma_core
        |from
        |    tb_ch ch
        |left outer join tb_ma ma
        |on ch.name=ma.name
        |""".stripMargin).show()








  }

}

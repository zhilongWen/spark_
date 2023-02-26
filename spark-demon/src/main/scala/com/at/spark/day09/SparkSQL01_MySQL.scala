package com.at.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.SaveMode

import java.util.Properties

/**
 * @author zero
 * @create 2021-03-21 21:16
 */
object SparkSQL01_MySQL {

  /*

      通过jdbc对MySQL进行读写操作

   */


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


/*

    var url: String = "jdbc:mysql://hadoop102:3306/test"
    var table: String = "user"
    var predicates: Array[String] = Array("user:root","password:000000")
    var connectionProperties: Properties = new Properties()
    connectionProperties.setProperty("driver","com.mysql.jdbc.Driver")
    val df: DataFrame = spark.read.jdbc(url, table, predicates, connectionProperties)
    df.show()
*/


/*
    val df: DataFrame = spark.read.format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url","jdbc:mysql://hadoop102:3306/test")
      .option("user","root")
      .option("password","000000")
      .option("dbtable","user")
      .load()
    df.show()
*/


/*
    spark.read.format("jdbc").options(
     Map(
       "url" -> "jdbc:mysql://hadoop102:3306/test?user=root&password=000000",
       "driver" -> "com.mysql.jdbc.Driver",
       "dbtable" -> "user"
     )
    ).load().show()
*/

/*
  //从MySQL数据库中读取数据  方式3
  val props: Properties = new Properties()
      props.setProperty("user","root")
      props.setProperty("password","000000")
      props.setProperty("driver","com.mysql.jdbc.Driver")
      val df: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop202:3306/test","user",props)
      df.show()
*/



    import spark.implicits._

    //向MySQL数据库中写入数据
    val rdd: RDD[User] = spark.sparkContext.makeRDD(List(User("banzhang", 20), User("jingjing", 18)))
//
//    val df: DataFrame = rdd.toDF()
//    df.write.format("jdbc")
//      .option("driver","com.mysql.jdbc.Driver")
//      .option("url","jdbc:mysql://hadoop102:3306/test")
//      .option("user","root")
//      .option("password","000000")
//      .option("dbtable","user")
//      .mode("append")
//      .save()

    //将RDD转换为DS
    val ds: Dataset[User] = rdd.toDS()

    ds.write.format("jdbc")
      .option(JDBCOptions.JDBC_DRIVER_CLASS,"com.mysql.jdbc.Driver")
      .option(JDBCOptions.JDBC_URL,"jdbc:mysql://hadoop102:3306/test?user=root&password=000000")
      .option(JDBCOptions.JDBC_TABLE_NAME,"user")
      .mode(SaveMode.Append)
      .save()

    spark.stop()

  }

}
case class User(name:String,age:Int)


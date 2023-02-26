package com.at.spark.day06

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, ResultSet}

/**
 * @author zero
 * @create 2021-03-16 23:36
 */
object Spark04_MySQL_read {

  /*

    注册驱动
    获取连接
    创建数据库操作对象 PrepareStatement
    执行SQL
    处理结果集
    关闭连接

   */

  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    var driver:String = "com.mysql.jdbc.Driver"
    var url:String = "jdbc:mysql://hadoop102:3306/test"
    var userName = "root"
    var password = "000000"

    var sql:String = "select * from student where id >= ? and id <= ?"

//      sc: SparkContext,   Spark程序执行的入口，上下文对象
//      getConnection: () => Connection,    获取数据库连接
//      sql: String,    执行SQL语句
//      lowerBound: Long,   查询的其实位置
//      upperBound: Long,   查询的结束位置
//      numPartitions: Int,   分区数
//      mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _  对结果集的处理
    new JdbcRDD(
      sc,
      () =>{
        Class.forName(driver)
        DriverManager.getConnection(url,userName,password)
      },
      sql,
      1,
      20,
      2,
      rs => (rs.getInt(1),rs.getString(2),rs.getInt(3))

    ).collect().foreach(println)


    // 关闭连接
    sc.stop()

  }

}

package com.at.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @author zero
 * @create 2021-03-16 23:47
 */
object Spark05_MySQL_write {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/test"
    var username = "root"
    var password = "000000"

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("jingjign",30),("guixianren",18)))

/*

    rdd.foreach{
      case (name,age) => {
        //注册驱动
        Class.forName(driver)
        //获取连接
        val conn: Connection = DriverManager.getConnection(url,username,password)
        //声明数据库操作的SQL语句
        var sql:String = "insert into student(name,age) values(?,?)"
        //创建数据库操作对象PrepareStatement
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //给参数赋值
        ps.setString(1,name)
        ps.setInt(2,age)
        //执行SQL
        ps.executeUpdate()
        //关闭连接
        ps.close()
        conn.close()
      }
    }
*/
    rdd.foreachPartition{
      //datas是RDD的一个分区的数据
      datas=>{
        //注册驱动
        Class.forName(driver)
        //获取连接
        val conn: Connection = DriverManager.getConnection(url,username,password)
        //声明数据库操作的SQL语句
        var sql:String = "insert into student(name,age) values(?,?)"
        //创建数据库操作对象PrepareStatement
        val ps: PreparedStatement = conn.prepareStatement(sql)

        //对当前分区内的数据，进行遍历
        //注意：这个foreach不是算子了，是集合的方法
        datas.foreach{
          case (name,age)=>{
            //给参数赋值
            ps.setString(1,name)
            ps.setInt(2,age)
            //执行SQL
            ps.executeUpdate()
          }
        }

        //关闭连接
        ps.close()
        conn.close()
      }
    }



    // 关闭连接
    sc.stop()
  }

  /*
  数据的读取和保存
	-textFile
	-sequenceFile
	-objectFile
	-Json
		本质还是通过textFile读取文本，对读到的内容进行处理
	-HDFS
	-MySQL
		map-------mapPartition
		foreach---foreachPartition
   */

}

package com.at.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.BufferedReader
import java.io.FileInputStream
import java.io.FileReader
import java.io.IOException
import java.util
import scala.collection.immutable


/**
 * @author zero
 * @create 2021-03-13 1:08
 *
 *
 */
object Spark01_CreateRDD {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Spark01_CreateRDD").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)


    val fileReader: FileReader = new FileReader("E:\\DOC\\BaiduNetdiskDownload\\2021.txt")

    val bufferedReader: BufferedReader = new BufferedReader(fileReader)

    val list = List()
//    val list: List[String] = new ArrayList[String]

    var line: String = null

    while ( { (line = bufferedReader.readLine) != null}) {

      println(line)
      immutable.List(line)
    }




    sc.stop()



  }

}

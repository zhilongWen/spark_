package com.at.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author zero
 * @create 2021-03-21 15:05
 */
object SparkSQL05_UDAF {

  /*
      自定义UDAF（弱类型  主要应用在SQL风格的DF查询）

      注意：如果从内存中获取数据，spark可以知道数据类型具体是什么，
           如果是数字，默认作为Int处理；但是从文件中读取的数字，不能确定是什么类型，所以用bigint接收，可以和Long类型转换，
           但是和Int不能进行转换
   */

  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL").setMaster("local[*]")

    //创建SparkSQL执行的入口点对象  SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //读取json文件创建DataFrame
    val df: DataFrame = spark.read.json("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input\\test.json")


    val myAvg = new MyAvg()

    spark.udf.register("myAvg",myAvg)

    df.createOrReplaceTempView("user")

    spark.sql("select myAvg(age) avgAge from user").show()



    spark.stop()


  }

}

//自定义UDAF函数(弱类型)
class MyAvg extends UserDefinedAggregateFunction{

  //聚合函数的输入数据的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("age",IntegerType)))
  }

  //缓存数据的类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("sum",LongType),StructField("count",LongType)))
  }

  //聚合函数返回的数据类型
  override def dataType: DataType = DoubleType

  //稳定性  默认不处理，直接返回true    相同输入是否会得到相同的输出
  override def deterministic: Boolean = true

  //初始化  缓存设置到初始状态
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //更新缓存数据
  override def update(buffer1: MutableAggregationBuffer, input: Row): Unit = {

    if(!buffer1.isNullAt(0)) {
      buffer1(0) = buffer1.getLong(0) + input.getInt(0)
      buffer1(1) = buffer1.getLong(1) + 1L
    }
  }


  //分区间的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  //计算逻辑
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }

}

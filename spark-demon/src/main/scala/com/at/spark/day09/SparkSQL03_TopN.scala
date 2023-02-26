package com.at.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import java.text.DecimalFormat

/**
 * @author zero
 * @create 2021-03-22 11:12
 */
object SparkSQL03_TopN {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    import spark.implicits._


    /*

      各区域热门商品Top3

        热门商品是从点击量的维度来看的，计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示

        地区	  商品名称	  点击次数	    城市备注
        华北	  商品A	    100000	    北京21.2%，天津13.2%，其他65.6%
        华北	  商品P     	80200	      北京63.0%，太原10%，其他27.0%
        华北  	商品M	    40000	      北京63.0%，太原10%，其他27.0%
        东北  	商品J	    92000	      大连28%，辽宁17.0%，其他 55.0%
        ...

     */

    spark.sql("use default")

    spark.udf.register("city_remark",new CityClickUDAF())


    //--1.1从用户行为表中，查询所有点击记录，并和city_info,product_info进行连接
    spark.sql(
      """
        |select
        |    b.*,
        |    c.product_name
        |from
        |    user_visit_action a
        |join
        |    city_info b
        |on
        |    a.city_id = b.city_id
        |join
        |    product_info c
        |on
        |    a.click_product_id = c.product_id
        |where
        |    a.click_product_id != -1
        |""".stripMargin).createOrReplaceTempView("t1")

    //--1.2按照地区和商品的名称进行分组，统计出每个地区每个商品的总点击数
    spark.sql(
      """
        |select
        |    t1.area,
        |    t1.product_name,
        |    count(*) as product_click_count,
        |    city_remark(t1.city_name)
        |from
        |    t1
        |group by t1.area,t1.product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    //1.3针对每个地区，对商品点击数进行降序排序
    spark.sql(
      """
        |select
        |    t2.*,
        |    row_number() over(partition by t2.area order by t2.product_click_count desc) cn
        |from
        |    t2
        |""".stripMargin).createOrReplaceTempView("t3")


    //--1.4取当前地区的前3名
    spark.sql(
      """
        |select
        |    *
        |from
        |    t3
        |where
        |    t3.cn <= 3
        |""".stripMargin).show(100,false)



    spark.stop()

  }

}


//自定义一个UDAF聚合函数，完成城市点击量统计
class CityClickUDAF extends UserDefinedAggregateFunction {

  //输入类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("city_name",StringType)))
  }

  //缓存类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("city_count",MapType(StringType,LongType)),StructField("total_count",LongType)))
  }


  //返回值类型
  override def dataType: DataType = {
    StringType
  }

  //稳定性
  override def deterministic: Boolean = false

  //缓存区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  //缓存数据更新
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val cityName: String = input.getAs[String](0)
    val cityMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)

    //更新城市
    buffer(0) = cityMap + (cityName -> (cityMap.getOrElse(cityName,0L) + 1L))

    //更新总点击数
    buffer(1) = buffer.getAs[Long](1) + 1L


  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)

    //城市合并
    buffer1(0) = map1.foldLeft(map2){
      case (mn,(k,v)) => {
        mn + (k -> (mn.getOrElse(k,0L) + v))
      }
    }

    //总数合并
    buffer1(1) = buffer1.getAs[Long](1) + buffer2.getAs[Long](1)

  }

  override def evaluate(buffer: Row): Any ={

    val cityCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getAs[Long](1)

    //取出前2
    val sortList: List[(String, Long)] = cityCount.toList.sortBy(-_._2).take(2)


    var citysRatio: List[CityRemark] = sortList.map {
      case (cityName, clickCount) => {
        CityRemark(cityName, clickCount.toDouble / totalCount)
      }
    }
    if(cityCount.size > 2){
      citysRatio = citysRatio :+ CityRemark("其他",citysRatio.foldLeft(1D)(_-_.cityRatio))
    }

    citysRatio.mkString(",")


  }



}

case class CityRemark(cityName: String, cityRatio: Double){

  val formatter = new DecimalFormat("0.00%")
  override def toString: String = s"$cityName:${formatter.format(cityRatio)}"
}
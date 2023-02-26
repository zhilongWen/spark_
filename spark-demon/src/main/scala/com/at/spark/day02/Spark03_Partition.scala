package com.at.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author zero
 * @create 2021-03-13 1:30
 */
object Spark03_Partition {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Spark03_Partition").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)



    //---------------从集合中创建RDD  默认分区大小 16
//    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    //自定义分区数
//    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5),3)
    /*
      设置分区数
        SparkContext$createTaskScheduler 方法中匹配时会创建 LocalSchedulerBackend 类 并初始化分区数
        默认的分区数为CPU的核数
          def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
          // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
          val threadCount = if (threads == "*") localCpuCount else threads.toInt
          if (threadCount <= 0) {
            throw new SparkException(s"Asked to run locally with $threadCount threads")
          }
          val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
          val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)

      分区规则 length集合size  numSlices分区数
         def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
            (0 until numSlices).iterator.map { i =>
              val start = ((i * length) / numSlices).toInt
              val end = (((i + 1) * length) / numSlices).toInt
              (start, end)
            }
          }

       真正将数据放入分区的是调用 ParallelCollectionRDD$compute方法


     */



    ////---------------读取外部文件创建RDD  默认分区大小 2
    val rdd: RDD[String] = sc.textFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input") //默认 def defaultMinPartitions: Int = math.min(defaultParallelism, 2) ==> Math.min(cpu核数,2)
    //自定义
//    val rdd: RDD[String] = sc.textFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\input\\3.txt",5)
    /*
      sc.textFile(文件路径) ==》 内部实际上是创建一个 HadoopRDD 调用getPartitions方法获取设置分区
      getPartitions中的val inputSplits = inputFormat.getSplits(jobConf, minPartitions) 实际上是调用FileInputFormat的split方法
      FileInputFormat$split
        获取大致的每个分区大小 long goalSize = totalSize / (long)(numSplits == 0 ? 1 : numSplits);
        如果 剩下的文件大小(开始时为文件大小)/每个分区大小 > 1.1 =》设置分区方法 =》剩下的文件大小=剩下的文件大小-每个分区大小
        否则 剩下的文件大小!= 0 将剩下的文件大小设置为一个分区

      真正将数据放入文件分区中的是 HadoopRDD$compute 方法
      compute 中的 inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL) 最终调用的是 LineRecordReader的next方法
      LineRecordReader有两个关键变量 start=split.getStart()	   end = start + split.getLength
      在next方法中读取文件是 int newSize = this.in.readLine(value, this.maxLineLength, Math.max(this.maxBytesToConsume(this.pos), this.maxLineLength));

     */

    //查看分区效果
//    println(rdd.partitions.size)


//    rdd.saveAsTextFile("D:\\workspace\\workspace2021\\bigdata\\spark\\spark-demon\\output")







    sc.stop()



    /*
      默认分区
        -从集合中创建RDD
          取决于分配给应用的CPU的核数
        -读取外部文件创建RDD
          math.min(取决于分配给应用的CPU的核数,2)
     */

    /*

      1.读取一个37个字节的文件默认分区为2 实际分区为多少？？？？？？？？？？？ ==》 2
      2.
abc
ef
g
hj
klm
读取这个文件 默认分区为5 实际分区为多少，每个分区内容有什么？？？？？？？？？？？？？ （换行为 \r\n 两个字节）
一共19个字节
0：0-3     0-3       abc
1：3-3     3-6       ef
2：6-3     6-9       g
3：9-3     9-12      hj
4：12-3    12-15     空
5：15-3    15-18     klm
6：18-1    18        空




     */



  }

}

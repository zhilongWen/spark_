package com.at.spark.day10

import org.apache.spark.SparkConf
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import scala.util.control.NonFatal

/**
 * @author zero
 * @create 2021-03-22 19:38
 */
object SparkStreaming03_CustomerReceiver {


  /*
      通过自定义数据源方式创建DStream

      模拟从指定的网络端口获取数据

   */

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))



    //通过自定义数据源创建Dstream
    val myDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 9999))

    val resDS: DStream[(String, Int)] = myDS.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    resDS.print()

    ssc.start()


    ssc.awaitTermination()


  }



}

//Receiver[T]  泛型表示的是 读取的数据类型
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  private var socket: Socket = _

  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }

  def receive() {
    try {

      socket = new Socket(host, port)

      val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream))

      var input:String = null

      while ((input = reader.readLine()) != null){
        store(input)
      }

    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    } finally {
      onStop()
    }
  }

  override def onStop(): Unit = {
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
      }
    }
  }
}
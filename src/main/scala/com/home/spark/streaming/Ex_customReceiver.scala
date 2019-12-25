package com.home.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

/**
  * @Author: xu.dm
  * @Date: 2019/12/24 13:45
  * @Version: 1.0
  * @Description: 自定义接收器
  **/
object Ex_customReceiver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf(true).setMaster("local[*]").setAppName("spark streaming wordcount")
    //优雅的停止SparkStreaming而不丢失数据
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")

    //环境对象，设置采集周期
    val scc: StreamingContext = new StreamingContext(conf,Seconds(30))
    // TODO: 可以通过ssc.sparkContext 来访问SparkContext或者通过已经存在的SparkContext来创建StreamingContext

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val socketStream: ReceiverInputDStream[String] = scc.receiverStream(new CustomReceiver("vmhome10.com",9999))

    val words: DStream[String] = socketStream.flatMap(_.split(" "))

    val pairs = words.map(word => (word,1))

    val wordCounts : DStream[(String, Int)] = pairs.reduceByKey(_+_)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print

    // Start the computation
    // 通过 streamingContext.start()来启动消息采集和处理
    scc.start()

    // Wait for the computation to terminate
    // 通过streamingContext.stop()来手动终止处理程序
    scc.awaitTermination()
  }

}

//模拟socket接收字符串
class CustomReceiver(hostname : String,port : Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){

  override def onStart(): Unit = {
    // Start the thread that receives data over a connection
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()
  }

  override def onStop(): Unit = {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  def receive(): Unit ={
    var socket : Socket = null
    var userInput : String = null

    try{
      socket = new Socket(hostname,port)

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))

      userInput = reader.readLine()
      while (!this.isStopped() && userInput!=null){
        this.store(userInput)
        userInput = reader.readLine()
      }
      reader.close()

      // Restart in an attempt to connect again when server is active again
      this.restart("Trying to connect again")
    }catch {
      case e : java.net.SocketException =>
        this.restart("Error connecting to " + hostname + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)

    }
    finally {
      if(socket!=null){
        socket.close()
        socket = null
      }
    }
  }


}

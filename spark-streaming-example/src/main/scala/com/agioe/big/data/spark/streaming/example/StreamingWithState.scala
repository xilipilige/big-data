package com.agioe.big.data.spark.streaming.example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * 带状态的流统计
  * @see: https://blog.csdn.net/jy02268879/article/details/81074307
  * @author yshen
  * @since 19-3-7
  */
object StreamingWithState {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("UpdateStateByKey")setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    /**如果使用了带状态的算子必须要设置checkpoint，
      * 这里是设置在HDFS的文件夹中
      */
    ssc.checkpoint("file:///root/sparkStreamingExampleFile/")

    val lines = ssc.socketTextStream("192.168.51.113",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1))

    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 把当前的数据去更新已有的数据
    * currentValues: Seq[Int] 新有的状态
    * preValue: Option[Int] 已有的状态
    * */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val currentCount = currentValues.sum//每个单词出现了多少次
    val preCount = preValues.getOrElse(0)

    Some(currentCount+preCount)//返回
  }


}

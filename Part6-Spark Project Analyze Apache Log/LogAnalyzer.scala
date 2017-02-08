package edu.mum.cs522.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object LogAnalyzer {

  def main(arg1: Array[String]): Unit = {

    //spark configuration
    val sparkConf = new SparkConf().setAppName("Log Analyze").setMaster("local[2]").set("spark.executor.memory", "1g");
    //spark context
    val sc = new SparkContext(sparkConf)

    val filename = arg1(0)

    //RDD Access Logs
    //You can use hdfs , but in this case I use local file
    val accessFile = sc.textFile(filename).map(LogParser.parseLog).cache()

    println("•••••••••••••••••••••••••••••••••••")
    println("•••••START APACHE LOG ANALYZER•••••")
    println("•••••••••••••••••••••••••••••••••••")


    //response code
    val responseErrorCode = accessFile.map(log => (log.responseCode, 1)).reduceByKey(_ + _).filter(_._1 >= 400).take(100)
    val responseSuccessCode = accessFile.map(log => (log.responseCode, 1)).reduceByKey(_ + _).filter(_._1 < 400).take(100)

    println("≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡")
    println("≡≡≡≡≡≡≡≡HTTP RETURN STATUS≡≡≡≡≡≡≡≡≡")
    println("≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡")
    println(s"""Error Resposnes: ${responseErrorCode.mkString("[", ",", "]")}""")
    println(s"""Success Response: ${responseSuccessCode.mkString("[", ",", "]")}""")
    println();

    //ipaddress of client access
    val ipAccess = accessFile.map(log => (log.ipAddress, 1)).reduceByKey(_ + _).filter(_._2 > 10).map(_._1).take(100)

    println("≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡")
    println("CLIENT IP ACCESS MORE THAN 10 TIMES")
    println("≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡")
    println(s"""${ipAccess.mkString("\n")}""")
    println()

    //Top 10 contents access
    val topContentAccess = accessFile.map(log => (log.endpoint, 1)).reduceByKey(_ + _).top(10)(Ordering.OrderValue)

    println("≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡")
    println("≡≡≡≡≡≡TOP 10 CONTENTS ACCESS≡≡≡≡≡≡≡")
    println("≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡")
    println(s"""${topContentAccess.mkString("\n")}""")
    println()

    //content size
    val contentSizes = accessFile.map(log => log.contentSize)
    println("≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡")
    println("≡≡≡≡AVERAGE OBJECT RETURN SIZE≡≡≡≡≡")
    println("≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡")
    println("Size of the Object Returned Avg:\t%s", contentSizes.reduce(_ + _) / contentSizes.count)
    println()

    println("•••••END OF RESULT•••••")

    sc.stop()

  }

}

object Ordering {
  object OrderValue extends Ordering[(String, Int)] {
    def compare(a: (String, Int), b: (String, Int)) = {
      a._2 compare b._2
    }
  }
}
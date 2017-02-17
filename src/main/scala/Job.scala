/**
  * Created by Matt on 17/02/2017.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Job {
  def main(args: Array[String]) {
    val logFile = "src/main/resources/SampleText.txt"
    val conf = new SparkConf().setAppName("SparkTest")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }
}

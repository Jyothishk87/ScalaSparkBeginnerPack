package spark1

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object groupBy extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "App1")
  val input = sc.textFile("/Users/jyothishk/Desktop/Big data training/bigLog.txt")

  val mappedIp = input.map( x=> {
    val fields = x.split(":")
    (fields(0),fields(1))
  })
  // this example to show limitations of groupBykey like under utilisized tasks
  mappedIp.groupByKey.collect().foreach(x => println(x._1,x._2.size))
  // below line is to hold on spark session and view jobs in localhost sparkUI
 // scala.io.StdIn.readLine()
}

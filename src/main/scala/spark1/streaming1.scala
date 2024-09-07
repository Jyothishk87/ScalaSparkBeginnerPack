package spark1
// dstream handling - only latest values (window size 5) will be received.
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.streaming.StreamingContext._

object streaming1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "streaming1")
  // ssc.checkpoint(".")
  val ssc = new StreamingContext(sc, Seconds(5))
  val lines = ssc.socketTextStream("localhost", 9998)
  val words = lines.flatMap(x => x.split(" "))
  val pairs = words.map(x => (x, 1))
  val wordCount = pairs.reduceByKey(_ + _)
  wordCount.print()

  ssc.start
  ssc.awaitTermination()

}

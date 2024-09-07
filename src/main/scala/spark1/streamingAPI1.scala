package spark1

import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object streamingAPI1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("StreamAPI1")
    .config("spark.sql.shuffle.partitions",3)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .getOrCreate()
  //read from the stream
   val lineDf = spark.readStream
     .format("socket")
     .option("host","localhost")
     .option("port","12345")
     .load()

  //process
  // basic word count example
  val wordsDf = lineDf.selectExpr("explode(split(value,' ')) as word")
  val countdf = wordsDf.groupBy("word").count()

  //write to sink
  val outputDf = countdf.writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation","checkpoint-location1")
    .start()
  outputDf.awaitTermination()

}

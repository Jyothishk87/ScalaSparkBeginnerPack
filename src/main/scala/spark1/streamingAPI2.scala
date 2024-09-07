package spark1

import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, from_json, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object streamingAPI2 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("StreamAPI1")
    .config("spark.sql.shuffle.partitions",3)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    //.config("spark.sql.streaming.schemaInference","true")
    .getOrCreate()
  //read from the stream
  val orderDf = spark.readStream
    .format("socket")
    .option("host","localhost")
    .option("port","12345")
    .load()

    val orderSchema = StructType(List(
      StructField("order_id",IntegerType),
      StructField("order_date",StringType),
      StructField("order_customer_id",IntegerType),
      StructField("order_status",StringType),
      StructField("amount",IntegerType)
    ))
  //process
  // Input from a socket has infered schema as a single coumn "value" of string type.
  val valueDf = orderDf.select(from_json(col("value"),orderSchema).alias("value"))

   //valueDf.printSchema()
  val refinedOrderDf = valueDf.select("value.*")
  // refinedOrderDf.printSchema()
  val windowaggDf = refinedOrderDf.groupBy(window(col("order_date"),"15 minute"))
    .agg(sum("amount")
    .alias  ("totalInvoice"))
 // windowaggDf.printSchema()
val outputDf = windowaggDf.select("window.*","totalInvoice")
  outputDf.printSchema()

  //write to sink
 val queryDf = outputDf.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation","checkpoint-location2")
   .trigger(Trigger.ProcessingTime("15 second"))
    .start()

  queryDf.awaitTermination()


}

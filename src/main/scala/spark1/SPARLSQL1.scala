package spark1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object SPARLSQL1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name" ,"Spark API1")
  sparkConf.set("spark.master","local[*]")

  val spark= SparkSession.builder().
    config(sparkConf)
    // appName("Spark API1").
    //   master("local[*]").
    .getOrCreate()

  val orderDataDf = spark.read.option("header",true)
    .format("csv")
    // infer schema is an action
    .option("inferSchema",true)
    .option("header",true)
    .option("path","/Users/jyothishk/Desktop/Big data training/week12datasets/orders.csv")
    // path and format can be given as below also.
    // .csv("/Users/jyothishk/Desktop/Big data training/week11datasets/orders.csv")
    .load()

 orderDataDf.createOrReplaceTempView("orders")
  val resultsdf = spark.sql("select order_customer_id,count(*)  as total_orders from orders where "+
  "order_status ='CLOSED' group by order_customer_id order by total_orders desc")

  resultsdf.show()

  Logger.getLogger(getClass.getName).info("app processed")


  spark.stop()

}


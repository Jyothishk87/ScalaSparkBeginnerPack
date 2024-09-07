package spark1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object dataframes extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name" ,"Spark API1")
  sparkConf.set("spark.master","local[*]")

  val spark= SparkSession.builder().
    config(sparkConf)
   // appName("Spark API1").
   //   master("local[*]").
      .getOrCreate()

   val orderDf = spark.read.option("header",true)
     .format("csv")
     // infer schema is an action
      .option("inferSchema",true)
     .option("header",true)
     .option("path","/Users/jyothishk/Desktop/Big data training/week11datasets/orders.csv")
    // path and format can be given as below also.
   // .csv("/Users/jyothishk/Desktop/Big data training/week11datasets/orders.csv")
     .load()


  val groupdorderDF= orderDf
     .repartition(4)
    .select("order_id","order_customer_id")
    .where("order_customer_id > 10000")
    .groupBy("order_customer_id")
    .count()

   //groupdorderDF.show()
   // below code is example for df write, no transformation applied
     orderDf.write
       .format("csv")
       .mode(saveMode = "OverWrite")
       .option("path","/Users/jyothishk/Desktop/Big data training/DFOPfolder1")
       .save()

  Logger.getLogger(getClass.getName).info("app processed")

 // orderDf.printSchema()
  spark.stop()

}


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp


object Datasets1 extends  App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Spark API1")
  sparkConf.set("spark.master", "local[*]")

  case class Orderdata (order_id:Int,order_date:Timestamp ,order_customer_id:Int,order_status:String)

  val spark = SparkSession.builder().
    config(sparkConf)
    // appName("Spark API1").
    //   master("local[*]").
    .getOrCreate()

  val orderDf = spark.read
    .option("header", true)
    // infer scgema is an action
    .option("inferSchema", true)
    .csv("/Users/jyothishk/Desktop/Big data training/week11datasets/orders.csv")
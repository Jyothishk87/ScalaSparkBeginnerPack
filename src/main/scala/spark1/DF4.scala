package spark1
// scala data structure to DF
// casting data type
// adding new columns
//dropping
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType

object DF4 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Spark API1")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().
    config(sparkConf)
    // appName("Spark API1").
    //   master("local[*]").
    .getOrCreate()

  // input is hard coded list
   val list1 = List(
     (1,"2023-02-01",11579,"CLOSED"),
     (2,"2023-02-03",82579,"PENDING"),
     (3,"2023-01-07",82463,"COMPLETE"),
     (4,"2023-01-09",54679,"CLOSED")
   )
   import spark.implicits._

  // converting list to rdd, then convert RDD to DF
   // val rdd = spark.sparkContext.parallelize(list1)
    //val df = rdd.toDF()
  // list can be directly converted todf using
  val statusDf = spark.createDataFrame(list1).toDF("orderId","orderDate","customerid","status")
  // converting/casting datatype
  // .withColumn can be used to update or add new column
   val statusNewDf= statusDf.withColumn("orderDate",unix_timestamp(col("orderDate").cast(DateType)))
    .withColumn("newID",monotonically_increasing_id())
    .dropDuplicates("status")
    .drop("orderId")
    .sort("orderDate")

  statusNewDf.show()


   spark.stop()


}

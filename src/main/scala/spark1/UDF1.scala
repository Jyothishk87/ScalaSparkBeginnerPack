package spark1
// UDF example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, expr, udf}

object UDF1 extends  App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "UDF1")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().
    config(sparkConf)
    // appName("Spark API1").
    //   master("local[*]").
    .getOrCreate()
  case class person (name:String, age:Int ,city: String)

  // udf function def

  def ageCheck(age:Int) = {
    if (age > 18 ) "Y" else "N"
  }

 val file1 = spark.read
   .format("csv")
   .option("inferSchema",true)
   .option("path","/Users/jyothishk/Desktop/Big data training/week12datasets/dataset1.csv")
   .load()

   import spark.implicits._

  val fileIp = file1.toDF("name","age","city")
  // register UDF as -column object expression. UDF wont added to catlog functions
   val ageChkUdf1 = udf(ageCheck(_:Int):String)
  // another way is SQL/string expression, here instead of defined function anonymous function can also be supplied to udf
  // in this case udf function added to catalog so can be used in spark SQL
    spark.udf.register("ageChkUdf" ,ageCheck(_:Int):String)
  //spark.udf.register("ageChkUdf",(x:Int) => { if (x >18) "1" else "0"})
  val fileOp1 = fileIp.withColumn("Adult",expr("ageChkUdf(age)"))
  val fileOp = fileIp.withColumn("Adult",ageChkUdf1(col("age")))
  //fileOp1.show()
  fileIp.createOrReplaceTempView("peopleTable")
  spark.sql("select name,age,city,ageChkUdf(age) as adult from peopletable ").show()

  spark.stop()
}

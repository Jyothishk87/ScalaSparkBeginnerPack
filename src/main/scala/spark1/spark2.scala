package spark1

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object spark2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "wordount")
  val input = sc.textFile("/Users/jyothishk/Desktop/Big data training/spark session 1 datasets/moviedata.data")
  val mappedip = input.map(x => x.split("\t")(2))
  // val ratings = mappedip.map(x => (x, 1))
  // val reducdedratings= ratings.reduceByKey((v1, v2) => (v1 + v2))

  // reducdedratings.collect().foreach(println)
  // map + reduceBykey => countByvalue
  val results = mappedip.countByValue
  results.foreach(println)

}

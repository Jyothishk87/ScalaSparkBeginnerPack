package spark1

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object spark3 extends App {

  def parseLine( line: String) = {

    val fields = line.split("::")
    val age = fields(2).toInt
    val numFrnds = fields(3).toInt
    (age,numFrnds)

  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "wordount")
  val input = sc.textFile("/Users/jyothishk/Desktop/Big data training/spark session 1 datasets/friendsdata.csv")
  val mappedip = input.map(parseLine)
  // val mapped2 = mappedip.map(x => (x._1,(x._2,1)))
  // mapValues pick only value part of key value tupple.
  val mapped2 = mappedip.mapValues(x => (x,1))
  val totalByAge=  mapped2.reduceByKey((v1,v2) => (v1._1+v2._1 , v1._2+v2._2))
  // val avgFrnds = totalByAge.map(x => (x._1,x._2._1/x._2._2)).sortBy(x => x._2)
  // above statement  simplified by using mapValues instead of map
   val avgFrnds = totalByAge.mapValues(x => (x._1/x._2)).sortBy( x => x._2)
   avgFrnds.collect.foreach(println)

}

package spark1
// This sample program shows usage of broadcast variable in spark
//a set of keywords can be broad-casted to all worker nodes to avoid processing these variables in the  distributed file.
// Useful to filter out useless keywords. Since the size of broadcast variables are minimal compare to the big data file,
// no cost on in memory of worker node is involved.
// Below example filter out boring keywords from search result files to get optimized cost calculation on search keyword

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source
object sparkbroadcastvar extends App {

    def loadBoringWords(): Set[String] = {
      var boringWords:Set[String] = Set()
      val lines = Source.fromFile("/Users/jyothishk/Desktop/Big data training/boringwords.txt").getLines()
      for (line <- lines)
        {
          boringWords += line
        }
        boringWords
    }

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "wordount")
    val input = sc.textFile("/Users/jyothishk/Desktop/Big data training/bigdatacamp.csv")
    var nameSet = sc.broadcast(loadBoringWords)
    val mappedip = input.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
    val words    = mappedip.flatMapValues(x => x.split(" "))
    val finalMapped = words.map(x=> (x._2.toLowerCase(),x._1))
  // negation is required to consider only variables not in nameset
    val filteredRDD= finalMapped.filter(x => !nameSet.value(x._1))
     val total = filteredRDD.reduceByKey((v1, v2) => (v1 + v2))
     val sorted = total.sortBy(x=> x._2,false)
    // sorted.foreach(println)
      sorted.saveAsTextFile("/Users/jyothishk/Desktop/Spark_op")

  }

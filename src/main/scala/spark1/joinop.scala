package spark1

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object joinop extends  App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "Joinop")
  val ratingsRDD = sc.textFile("/Users/jyothishk/Desktop/Big data training/week11datasets/ratings.dat")
  val ratingMPD= ratingsRDD.map(x =>
  {
    val fields = x.split("::")
    (fields(1),fields(2))

  })
  // the aboved ratingMPD looks like  (1193,5)
  // (1193,4) (1193,3) ..
  // do mapValues to make (1193,(4.0,1.0))  (1193,(5.0,1.0)) etc

  val newratingMPD= ratingMPD.mapValues(x => (x.toFloat,1.0))
  // need to sum up rating and its count . use reduceByKey

  val reducedRating = newratingMPD.reduceByKey((x,y) => ((x._1+y._1,x._2+y._2)))
  // filter out movies which are atleast rated 1000 times
  val ratingfilt = reducedRating.filter(x => (x._2._2 > 1000))
  // average rating above 4.5
  val avgRatingFiltered = ratingfilt.mapValues(x =>(x._1/x._2)).filter((x => x._2 > 4))
  // avgRatingFiltered.collect().foreach(println)
 //to get the movie name , need to join above result with titles file.
 val moviesRDD = sc.textFile("/Users/jyothishk/Desktop/Big data training/week11datasets/movies.dat")
 val moviesMPD = moviesRDD.map( x =>{
   val fileds2 = x.split("::")
   (fileds2(0),fileds2(1))
 })

  val joinedRDD = moviesMPD.join(avgRatingFiltered)
 // joinedRDD.collect().foreach(println)
  val topMovies = joinedRDD.map(x=> (x._2._1))
  topMovies.collect().foreach(println)
  // below line is to hold on spark session and view jobs in localhost sparkUI
  scala.io.StdIn.readLine()

}

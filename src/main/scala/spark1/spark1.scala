package spark1

import org.apache.spark.SparkContext

object spark1 extends App {

  val sc = new SparkContext("local[*]","wordount")
  val input = sc.textFile("/Users/jyothishk/Desktop/Big data training/spark session 1 datasets/file1.txt")
  val mappedip = input.flatMap(x => x.split( " "))
  val KV = mappedip.map(x => (x,1))
  val reducdedKV = KV.reduceByKey((v1,v2) => (v1+v2))

   reducdedKV.collect().foreach(println)







}

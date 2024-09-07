package spark1
// previous rdd values are saved in prev values.
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming._
object streaming2 extends  App{
  val sc = new SparkContext("local[*]", "streaming2")
  val ssc = new StreamingContext(sc, Seconds(5))
  ssc.checkpoint(".")
  val lines = ssc.socketTextStream("localhost", 9998)

  def updatefunc(newValues: Seq[Int] , prevValues: Option[Int]):
  Option[Int] = {
    val newCount = prevValues.getOrElse(0) + newValues.sum
    Some(newCount)
  }
  val words = lines.flatMap(x => x.split(" "))
  val pairs = words.map(x => (x, 1))
  val wordCount = pairs.updateStateByKey(updatefunc)
  wordCount.print()

  ssc.start()

  ssc.awaitTermination()

}

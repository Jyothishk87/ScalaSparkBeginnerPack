package spark1
import scala.io.StdIn._

// get a list of numbers and K , print count of numbers greater than K
object week8q2 extends App {
  val ipNumbers = readLine
  val ipK       = readInt()
  val ipArray = ipNumbers.split(" ").map(x => x.toInt)

  var count = 0
  for (x <- ipArray) {
    if ( x > ipK ) {
      count = count +1
    }

  }
  println(count)
}

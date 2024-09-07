package spark1
import scala.io.StdIn._
// find out perfect squares from entered bill amounts
// save input number.
object week8q1 extends App {
  val numbCust:Int = readInt()
  val billAmnts: String = readLine()
  val billAmnt: Array[Int] = billAmnts.split(" ").map(x => x.toInt)

   var count = 0
   for (i <- billAmnt){
     val sqrt = Math.sqrt(i)
     val Ceil = sqrt.ceil
     if (sqrt.ceil - sqrt ==0){
       count = count + 1
     }
   }
  println(count)
}

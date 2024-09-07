package spark1
import scala.io.StdIn._
// string reverse for an input line
//def main (args : Array[String])={}
object stringReverse extends App {

  val inLine = readLine()
  val op1   = inLine.reverse
  println(op1)
  val op2  = inLine.split(" ").map(x => x.reverse)
  println(op2.mkString(" "))

  val op3 = inLine.split(" ").reverse
  println(op3.mkString(" "))

}

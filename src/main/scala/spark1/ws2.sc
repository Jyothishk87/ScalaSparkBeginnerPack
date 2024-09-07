 object scalaLearning2 {
   //function
   // return type Int is optional
   // {} also optiona
   // def squareIt(x:Int) = x*x
   def squareIt(x:Int) : Int = {
    x*x
   }
   def cubeIt (x:Int) = x*x*x
   println(squareIt(4))
   // functions can take other function as input :Higher order function

   def transformIt(x:Int, f:Int => Int ) =
     { f(x)}

   transformIt(5,squareIt)
   transformIt(6,cubeIt)
   // can supply an anonymous function as well
   transformIt(7,x => x*x*x)
 }

 //======================================
 // a fucntion can be assigned to a variable

 def doubler(i:Int) = {
   i+i
 }
  var a = doubler(_)
    println(a(2))

var b = 1 to 10

b.map(doubler)
// function with name is anonymous function, lamda in python.

 // Recursion & Loop
 // factorial example
   def fact1(ip  :Int) ={
   var result = 1
   for(i <- 1 to ip) {
     result = result * i
   }
    result

 }

println(fact1(9))

 // recursion

 def fact2(ip: Int):Int = {
   if (ip==1)
     1
   else
     ip*fact2(ip-1)
 }
// function currying & partial function

 def maybeString(num:Int): Option[String] ={
   if (num > 0) Some("An integer")
   else None
 }

 def printResult(num:Int) = {
   maybeString(num:Int) match {
     case Some(str) => println(str)
     case None => println("No string")
   }
 }

 // string interpolatation
 printResult(6)
 var name:String = "Jyo"
 println(s"hello $name how are you " )

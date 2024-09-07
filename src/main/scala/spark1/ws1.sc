object scalalearning {

  val a : String = "hello there"
  println(a)
  // type inference.
  val n1 = 5
  val c : Boolean = true
  val d : Char= 'a'
  val e : Int = 99
  val pi  = 3.145
  // == can be used for string comparison
  val n2= "jyo"
  val n3 = "jyo"
  val z = n2==n3
 // if else
  // match case (switch in java)
  val num = 1
  num match {
    case 1 => println("one")
    case 2 => println("two")
    case _ => println("something")
  }
    //for loop
    for (x <- 1 to 10)
      {
        println(x)
      }
    for (y <- 5 to 10)
      {println(y*y)}
    // while loop
      var i = 5
    while (i <= 10)
      { println(i*3)
        i = i+1
      }
  // do while

   do {
     println(i*i)
     i = i+2
   } while (i < 20)
}

// scala collections
//ARRAY
 val A1 = Array(1,2,3,4)
 for (i <- A1) println(i)
 A1(1)
// Array is mutab;le so value can be changed.
// searching based on index is fast in Array.Adding new element is inefficient
//---------------------------------
//LIST --> single linkedlist contain elements of same data type.
// search is not efficient but adding new item
val L1= List(1,2,3,4,5,6)
println(L1.head)
println(L1.tail)

for(i <- L1) println(i)
L1.reverse

//TUPLE
// elements of different data types in tuple, comparable with a record in db

val T1 = ("Jyo",35,'M',true)
// tuple is 1 based unlike list and array which is 0 based
 println(T1._3)
// tuple of 2 is key value pair in scala spark,widely used.
val T2 = (101  , "jyo")



//RANGE
val rng = 5 to 10
for (i <- rng) println(i)


//SET - holds unique value, order is not maintained

val s1 = Set(45,32,45,56,67,3,4)

//

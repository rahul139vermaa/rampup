package batchrampup



/**
  * Created by rahul on 11/11/16.
  */
object MainClass {

  def main(args: Array[String]) : Unit= {
    if(args.length<4) {println("please enter \n " +
      "file path \n" +
      "file uri \n"+
      "number of required person objects \n" +
      "time to sleep after execution") ; return }

    Constants.FILE_PATH=args(0)
    Constants.HDFS_URI=args(1)
    D1.Ob1(args(2).toInt)
    Thread.sleep(30000l)
    D1.Ob2
    D2WithRDD.Ob2
    D2WithDF.Ob2
    Thread.sleep(args(3).toLong)
 /*   println(fact(5))
    println(factByGenPS(5))
    val x=new Rational(2,3)
    println(x.numer)*/

  }

/*

  class Rational(n:Int,d:Int){
    def numer=n
    def denom=d
}

  def factByGenPS(a: Int) = mapReduce(x => x, (x, y) => x * y,1)(1, a) // if (a > b) 1 else a * genPS(f,cf,uv)(a + 1, b)



  def fact(a: Int) = product(x => x)(1, a)

  def product(f: Int => Int)(a: Int, b: Int): Int = mapReduce(f,(x,y) => x*y,1)(a,b)

  def mapReduce(f:Int => Int, cf:(Int,Int) =>Int, uv:Int)(a: Int, b: Int) : Int={
    if (a > b) uv else cf(f(a), mapReduce(f,cf,uv)(a + 1, b))
  }

  def product1(f: Int => Int)(a: Int, b: Int): Int = {
    if (a > b) 1 else f(a) * product(f)(a + 1, b)
  }

  def summ1(f: Int => Int): (Int, Int) => Int = {
    def s(a: Int, b: Int): Int =
      if (a > b) 0 else f(a) + s(a + 1, b)
    s
  }

  def summ(f: Int => Int)(a: Int, b: Int): Int = {
    if (a > b) 0 else f(a) + summ(f)(a + 1, b)
  }


  def sum(f: Int => Int)(a: Int, b: Int): Int = {
    def loop(a: Int, acc: Int): Int = {
      if (a > b) acc
      else loop(a + 1, f(a) + acc)
    }
    loop(a, 0)
  }
*/



}

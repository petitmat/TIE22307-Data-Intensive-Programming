package weeklyExercise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Main extends App {
  val conf = new SparkConf().setMaster("local").setAppName("ex2")
  val sc = new SparkContext(conf)

  // There are three scientific articles in the directory src/main/resources/articles/
  // The call sc.textFile(...) returns an RDD consisting of the lines of the articles:
  val articlesRdd: RDD[String] = sc.textFile("src/main/resources/articles/*")


  // Task #1: How do you get the first 10 lines as an Array
  val lines10 = articlesRdd.take(10)

  // Task #2: Compute how many lines there are in the articles
  val nbrOfLines = articlesRdd.count()
  println(f"#lines = ${nbrOfLines}%6s")

  // Task #3: What about the number of words
  val words =
    (for {
      line <- articlesRdd
    } yield {
      val count = line.split(" ")
      count.size
    }).sum
  println(f"#words = ${words}%6s")

  // Task #4: What is the number of chars?
  val chars =
    (for {
      line <- articlesRdd
    } yield {
      line.length
    }).sum
  println(f"#chars = ${chars}%6s")

  // Task #5: How many time the word 'DisCo' appears in the corpus?
  val disco =
    (for {
      line <- articlesRdd
      if line.contains("DisCo")
    } yield {
      line.sliding("DisCo".length).count(_ == "DisCo")
    }).sum
  println(f"#disco = ${disco}%6s")

  // Task #6: How do you "remove" the lines having only word "DisCo". Can you do it without filter-function?
  val noDisCoLines = articlesRdd.filter(! _.contains("DisCo"))

  // Pretend that 'nums' is a huge rdd of integers.
  val nums: RDD[Int] = sc.parallelize(List(2,3,4,5,6,7,8,9,10))

  // You are given a factorization function:
  def factorization(number: Int, list: List[Int] = List()): List[Int] = {
    for(n <- 2 to number if (number % n == 0)) {
      return factorization(number / n, list :+ n)
    }
    list
  }

  // Task #7: Compute an rdd containing all factors of all integers in 'nums'
  val allPrimes = nums.flatMap(factorization(_))


  // Task #8: Print all the values in allPrimes
  allPrimes.foreach(println(_))


  // Bonus task:
  // Here is the code snippet which was already in the first exercises. Explain how it works.
  // You can use http://www.scala-lang.org/api/2.11.8/
  val sheena = "sheena is a punk rocker she is a punk punk".split(" ").map(s => (s, 1)).groupBy(p => p._1).mapValues(v => v.length)
  sheena.foreach(println)

  // This function count the numbers of occurences of each words of a string 

}

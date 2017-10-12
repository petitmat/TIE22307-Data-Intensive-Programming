package exercise
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

case class Customer(custId: Int, name: String, streetAddr: String, postOffic: String)


import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._

object ex5 extends App{
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)

  // Create a SparkSession object:
  val spark = SparkSession.builder()
			  .appName("ex5")
			  .config("spark.driver.host", "localhost")
			  .master("local")
			  .getOrCreate()
                                                  

  // sqlContext.implicits._ are (Scala-specific) implicit methods available in Scala for converting common Scala objects into DataFrames:
  val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
  import sqlContext.implicits._
	
  // jsonl ending in a file denotes that the file has a single json object on each line.
  // The directory src/main/resources/sport contains training files in jsonl format.
	
  // Task #1: Read the files as a DataFrame:
  val trainingDf = spark.read.json("src/main/resources/sport/training1.jsonl","src/main/resources/sport/training2.jsonl","src/main/resources/sport/training3.jsonl")
  println("trainingDf")
  trainingDf.show

  // Task #2: What is the schema of trainingDf
  println("Schema of trainingDf")
  trainingDf.toDF("averageHR","comment","length", "maxHR","sportType","userId")

  // Task #3: Select only the lines which are related to running from trainingDf.
  val onlyRunning = trainingDf.where("sportType = 'running'")
  println("onlyRunning")
  onlyRunning.show


  // Task #4: Group the data in trainingDf by the sport type and compute the maximum of maxHR for each sport.
  val maxHR = trainingDf.groupBy("sportType").max("maxHR")
  println("maxHR")
  maxHR.show
  

  // Let us pretend that have a company that charges the customers based on the amount of downloaded data from our web service.
  // The company uses Apache web server. Therefore, the billing is based on the web server logs.

  // Apache web server logs all the HTTP requests to the service. Below is an example of a single row in the log:
  // 130.230.4.2 - - [07/Mar/2016:16:20:55 -0800] 'GET /api/customer/123/data/abc HTTP/1.1' 200 5203
  //   130.230.4.2 is the IP address of the client (remote host) which made the request to the server.
  //   Hyphens (-) mean missing information.
  //   [07/Mar/2016:16:20:55 -0800] is time that the request was received
  //   'GET /api/customer/123/data/abc HTTP/1.1' is the request
  //   200 is the success code sent back to the client (200 means OK)
  //   5203 is the size of the object returned to the client

  
  // Here is the Apache log. (In reality, we would have many log files on different machines, and each file would be large): 
  // 130.230.4.2 - - [07/Mar/2016:16:20:55 -0800] 'GET /api/customer/123/data/abc HTTP/1.1' 200 5203
  // 77.240.19.27 - - [08/Mar/2016:12:20:55 -0800] 'GET /api/customer/123/data/abc HTTP/1.1' 200 4103
  // 130.230.4.2 - - [07/Mar/2016:16:20:55 -0800] 'GET /api/customer/456/data/def HTTP/1.1' 200 3456
  // 54.209.46.155 - - [07/Mar/2016:16:49:57 -0800] 'POST /api/customer/123/data HTTP/1.1' 200 30
  // ...

  // From this data we need only the customer id and the returned object sizes. Moreover, we are only interested in GET requests.
  // DataFrame logDf has this information:
  val logDf = spark.sparkContext.parallelize(List((123, 5203), (123, 4103), (456, 3456))).toDF("custId", "amount")
  logDf.show



  // The customer data of the company is in DataFrame of Customers (The class Customer is defined on the top of this file).
  val customerRdd = spark.sparkContext.parallelize(List(Customer(123, "Theresa", "10 Downing Street", "London"),
			                                Customer(456, "Harry", "4 Privet Drive", "Little Whinging"),
			                                Customer(789, "Sherlock", "221B Baker Street", "London")))
  val customerDf = customerRdd.toDF


  // Task #5: Join logRdd and customerRdd using the customer id as the join condition.
  // See 'join(right: Dataset[_], joinExprs: Column)' on page https://spark.apache.org/docs/2.1.1/api/scala/index.html#org.apache.spark.sql.Dataset
  // Hint: You can refer to column custId in logDf like this: logDf("custId")
  val billingDf = logDf.join(customerDf, logDf("custId") === customerDf("custId"))
  println("billingDf")
  billingDf.show


  // Task #6: Calculate the total amount of downloaded data for each customer
  val totalDf = billingDf.groupBy(customerDf.col("custId")).agg(sum("amount"))
  println("totalDf")
  totalDf.show


  // Bonus Task #1: Use outer join to find the customers who a have not downloaded any data
  val passiveCustomersDf = logDf.join(customerDf,logDf("custId") === customerDf("custId"),"outer").where(logDf.col("custId").isNull)
  println("passiveCustomersDf")
  passiveCustomersDf.show


  // Bonus Task #2: In the Task #5 you were given logDf DataFrame. In reality logDf is created from Apache log lines - see RDD rawLog below.
  // Transform the rawLog to DataFrame logDf2 so that it has the same schema and content as logDf.
  val rawLog = spark.sparkContext.parallelize(List("130.230.4.2 - - [07/Mar/2016:16:20:55 -0800] 'GET /api/customer/123/data/abc HTTP/1.1' 200 5203",
                                                   "77.240.19.27 - - [08/Mar/2016:12:20:55 -0800] 'GET /api/customer/123/data/abc HTTP/1.1' 200 4103",
                                                   "130.230.4.2 - - [07/Mar/2016:16:20:55 -0800] 'GET /api/customer/456/data/def HTTP/1.1' 200 3456",
                                                   "54.209.46.155 - - [07/Mar/2016:16:49:57 -0800] 'POST /api/customer/123/data HTTP/1.1' 200 30"))


  val logRdd: RDD[(String, Int)] = rawLog.map(p => ("""(?<=customer\/)[0-9]*""".r.findFirstIn(p).getOrElse("000").toString,"""[0-9]{1,4}$""".r.findFirstIn(p).getOrElse("0").toInt))
  val logDf2 = logRdd.toDF("custId", "amount")
  println("logDf2")
  logDf2.show

}
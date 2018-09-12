package helwig.joerg.SparkLogFileNASA

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object LogFileAnalisisLocalApp {
  
	

	
  def main(args: Array[String]) {
        
        if (args.length != 1) {
            println("Expected:1 , Provided: " + args.length)
            return;
        }
	  
  val (inputFile) = (args(0))
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("log file analysis")

    val sc = new SparkContext(conf)
   sc.setLogLevel("INFO")
	
     val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()
    //val logFile = sc.textFile("/data/spark/project/NASA_access_log_Aug95.gz")	
    val acessLogFile = sc.textFile(inputFile)	
   process(acessLogFile, session)		  

    
    }	
	


 
 case class LogRecord( host: String, timeStamp: String, url:String,httpCode:Int)

val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

  def process(logFile:RDD[String] ,  session : SparkSession) {
	 
	  import session.implicits._  
  
   
  //val logFileInput = sc.textFile("/data/spark/project/NASA_access_log_Aug95.gz")
  val accessLog = logFile.map(parseLogLine)
  val accessDf = accessLog.toDF()
  accessDf.printSchema
  val inputData=prepareData(accessDf, session) 

  topLogRecord(inputData,session).show
  println("topLogRecord")	  
	  
 
  highTrafficWeefDay(inputData, session).show
  println("highTrafficWeefDay")

  lowTrafficWeefDay(inputData, session).show
  println("lowTrafficWeefDay")	  
	  

  highTrafficHour(inputData, session).show
  println("highTrafficHour")	  
	  
  
  lowTrafficHour(inputData, session).show
  println("lowTrafficHour")	  
	  
 
  countByHTTP(inputData, session).show
  println("countByHTTP")	  
  
  
  }




def parseLogLine(log: String) : 
 LogRecord = {  
   log match {  case PATTERN(host, group2, group3,timeStamp,group5,url,group7,httpCode,group8) => LogRecord(s"$host",s"$timeStamp",s"$url", s"$httpCode".toInt) 
		case _ => LogRecord("Empty", "", "",  -1 )}}

def prepareData (input: DataFrame, session : SparkSession ): DataFrame = {
 import session.implicits._
 input.select($"*").filter($"host" =!= "Empty").withColumn("Date",unix_timestamp(input.col("timeStamp"), "dd/MMM/yyyy:HH:mm:ss").cast("timestamp")).withColumn("unix_ts" , unix_timestamp($"Date") ).withColumn("year", year(col("Date"))).withColumn("month",month(col("Date"))).withColumn("day", dayofmonth(col("Date"))).withColumn("hour", hour(col("Date"))).withColumn("weekday",from_unixtime(unix_timestamp($"Date", "MM/dd/yyyy"), "EEEEE"))
}

def topLogRecord(input: DataFrame, session : SparkSession): DataFrame = {
	 import session.implicits._
	input.select($"url").filter(upper($"url").like("%HTML%")).groupBy($"url").agg(count("*").alias("cnt")).orderBy(desc("cnt")).limit(10)
    
}

 def highTrafficWeefDay (input: DataFrame, session : SparkSession): DataFrame = 
	{
        import session.implicits._
	 input.select($"weekday").groupBy($"weekday").agg(count("*").alias("count_weekday")).orderBy(desc("count_weekday")).limit(5)
	 
 }

 def lowTrafficWeefDay (input: DataFrame, session : SparkSession): DataFrame = {
	  import session.implicits._
	 input.filter($"weekday" =!= "").select($"weekday").groupBy($"weekday").agg(count("*").alias("count_weekday")).orderBy(asc("count_weekday")).limit(5)
	 
 }

def highTrafficHour (input: DataFrame, session : SparkSession): DataFrame = {
	  import session.implicits._
	 input.select($"hour").groupBy($"hour").agg(count("*").alias("count_hour")).orderBy(desc("count_hour")).limit(5)
	 
 }

def lowTrafficHour (input: DataFrame, session : SparkSession): DataFrame = {
	 import session.implicits._
	 input.select($"hour").groupBy($"hour").agg(count("*").alias("count_hour")).orderBy(asc("count_hour")).limit(5)
	 
 }

def countByHTTP (input: DataFrame, session : SparkSession): DataFrame = {
	  import session.implicits._
	 input.select($"httpCode").groupBy($"httpCode").agg(count("*").alias("count_httpCode"))
	 
 }
	
	
}

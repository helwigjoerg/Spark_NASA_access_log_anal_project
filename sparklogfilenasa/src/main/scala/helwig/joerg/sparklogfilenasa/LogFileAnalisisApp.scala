package helwig.joerg.SparkLogFileNASA

import org.apache.spark.{SparkConf, SparkContext}
import sqlContext.implicits._
import org.apache.spark.sql.SparkSession
import session.implicits._

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object LogFileAnalisisLocalAppApp extends App{
  val (inputFile) = (args(0))
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("log file analysis")

    val sc = new SparkContext(conf)
    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()

  Runner.run(conf, inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object LogFileAnalisisApp extends App{
  val (inputFile) = (args(0))

  // spark-submit command should supply all necessary config elements
  RunnerLogFile.run(new SparkConf(), inputFile)
}

object RunnerLogFile {
  def run(conf: SparkConf, inputFile: String): Unit = {
  val sc = new SparkContext(conf)

    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[1]").getOrCreate()
    val rdd = sc.textFile(inputFile)
    val counts = LogFileAnalisis.process (rdd)
    counts.saveAsTextFile(outputFile)
  }
}

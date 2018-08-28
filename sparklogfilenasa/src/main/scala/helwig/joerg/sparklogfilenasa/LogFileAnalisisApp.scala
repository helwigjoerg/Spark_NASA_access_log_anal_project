package helwig.joerg.SparkLogFileNASA

import org.apache.spark.{SparkConf, SparkContext}
import sqlContext.implicits._

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object LogFileAnalisisLocalAppApp extends App{
  val (inputFile) = (args(0))
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")

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
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val rdd = sc.textFile(inputFile)
    val counts = LogFileAnalisis.process (rdd)
    counts.saveAsTextFile(outputFile)
  }
}

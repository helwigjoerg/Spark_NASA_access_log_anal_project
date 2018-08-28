package helwig.joerg.SparkLogFileNASA

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object LogFileAnalisisApp extends App{
  val (inputFile) = (args(0))
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")

  Runner.run(conf, inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object LogFileAnalisisAppextends App{
  val (inputFile) = (args(0))

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputFile)
}

object Runner {
  def run(conf: SparkConf, inputFile: String): Unit = {
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(inputFile)
    val counts = LogFileAnalisis.process (rdd)
    counts.saveAsTextFile(outputFile)
  }
}

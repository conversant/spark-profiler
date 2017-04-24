import java.util.Date
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf
import Parser._
import Profiler._

/**
  * SummaryGenerator is an example of how the the Spark Profiler can be used.
  * Given a file containing application events, the SummaryGenerator extracts
  * application information and all task information and generates a summary
  * of the tasks. This kind of emulates the mapreduce job summary page.
  */
object SummaryGenerator {

  def printUsage(): Unit = {
    println
    println("============================================================")
    println("Usage: Need to provide the path to events log.")
    println("============================================================")
    println
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      printUsage()
      System.exit(1)
    }

    val eventsFile = args(0)

    val sparkConf = new SparkConf()

    if (!sparkConf.contains("spark.app.name")) {
      sparkConf.setAppName(this.getClass.getSimpleName)
    }

    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }

    val spark = new SparkSession.Builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val startTime = new Date

    println(s"Starting summary generation at ${startTime}")

    // Note that events is just a dataset containing the lines from the events file.
    // It does not actually parse any lines.
    val events = spark.read.textFile(eventsFile)

    val application = parseApplication(spark, events)

    println(s"Application name     : ${application.applicationName}")
    println(s"Application id       : ${application.applicationId}")
    println(s"Application duration : ${application.applicationDuration/1000} seconds")

    // executors not used.
    // Its just here to show how executor events can be extracted from events file.
    val executors = parseExecutors(spark, events, application)

    // Jobs summary
    val jobs = parseJobs(spark, events, application)
    jobProfile(spark, jobs).show

    // Stage summary
    val stages = parseStages(spark, events, jobs)
    stageProfile(spark, stages).show

    // Task summary has all the interesting metrics
    val tasks = parseTasks(spark, events, stages)
    val x = taskProfile(spark, tasks)

    x.select("summary", "taskDuration", "gcTime", "peakMemory").show()
    x.select("summary", "resultSize", "gettingResultTime", "resultSerializationTime").show()
    x.select("summary", "bytesRead", "recordsRead").show()
    x.select("summary", "memoryBytesSpilled", "diskBytesSpilled").show()
    x.select("summary", "fetchWaitTime", "totalRecordsRead").show()
    x.select("summary", "localBlocksFetched", "localBytesRead").show()
    x.select("summary", "remoteBlocksFetched", "remoteBytesRead").show()
    x.select("summary", "shuffleWriteTime", "shuffleBytesWritten", "shuffleRecordsWritten").show()
    x.select("summary", "inputRows", "outputRows").show()

    val endTime = new Date
    println(s"Summary generation completed at ${endTime}")
    println(s"Runtime = ${(endTime.getTime - startTime.getTime)/1000} seconds")

    spark.stop()
  }

}

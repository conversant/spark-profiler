import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf

/**
  * A simple example of "profiling".
  * This profiler simply performs a "describe" on appropriate
  * numeric values to provide a summary of the application.
  */
object Profiler {

  def jobProfile(ss: SparkSession, jobs: Dataset[Job]) = {
    import ss.implicits._
    jobs.
      where($"jobDuration".gt(0)).
      describe("jobDuration")
  }

  def stageProfile(ss: SparkSession, stages: Dataset[Stage]) = {
    import ss.implicits._
    stages.
      where($"jobDuration".gt(0)).
      describe("stageDuration")
  }

  /**
    * This does (all)most all of the interesting profiling
    * @param ss
    * @param tasks
    * @return
    */
  def taskProfile(ss: SparkSession, tasks: Dataset[Task]) = {
    import ss.implicits._
    tasks.
      where($"taskDuration".gt(0)).
      describe("taskDuration", "gcTime", "peakMemory",
        "gettingResultTime", "resultSerializationTime", "inputRows", "outputRows",
        "resultSize", "bytesRead", "recordsRead", "memoryBytesSpilled",
        "diskBytesSpilled", "shuffleBytesWritten", "shuffleRecordsWritten",
        "shuffleWriteTime", "remoteBlocksFetched", "localBlocksFetched",
        "fetchWaitTime", "remoteBytesRead", "localBytesRead", "totalRecordsRead")
  }


}

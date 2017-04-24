import org.apache.spark.sql.{Dataset, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  * Parser does all the work of parsing events from the application events file.
  */
object Parser {

  def parseJSON[T](json: String)(implicit t: Manifest[T]): T = {
    implicit val formats = DefaultFormats
    parse(json).extract[T]
  }

  /**
    * Parse application start and end events to get an {@link Application} instance.
    * @param spark
    * @param events
    * @return
    */
  def parseApplication(spark: SparkSession,
                       events: Dataset[String]): Application = {

    import spark.implicits._

    val startEvent = events.filter(_.contains("SparkListenerApplicationStart"))
    val endEvent = events.filter(_.contains("SparkListenerApplicationEnd"))
    val start = parseJSON[SparkApplicationStart](startEvent.head())
    val end = parseJSON[SparkApplicationEnd](endEvent.head())
    val applicationName = start.`App Name`
    val applicationId = start.`App ID`
    val applicationStartTime = start.`Timestamp`
    val applicationEndTime = end.Timestamp
    val jobDuration = applicationEndTime - applicationStartTime
    Application(applicationName, applicationId, applicationStartTime, applicationEndTime, jobDuration)
  }

  /**
    * Parse executor add event to return a dataset of {@link Executor}
    * @param spark
    * @param events
    * @param sparkApplication
    * @return
    */
  def parseExecutors(spark: SparkSession,
                     events: Dataset[String],
                     sparkApplication: Application): Dataset[Executor] = {

    import spark.implicits._

    events.filter(_.contains("SparkListenerExecutorAdded")).
      map(string => parseJSON[SparkExecutorAdded](string)).
      map(e => {
        val executorId = e.`Executor ID`
        val host =   e.`Executor Info`.`Host`
        val cores = e.`Executor Info`.`Total Cores`
        val startTime = e.`Timestamp`
        Executor(sparkApplication.applicationName,
          sparkApplication.applicationId,
          executorId, host, cores, startTime)
      })
  }

  /**
    * Parse job start and end events to return a dataset of {@link Job}
    * @param spark
    * @param events
    * @param sparkApplication
    * @return
    */
  def parseJobs(spark: SparkSession,
                events: Dataset[String],
                sparkApplication: Application): Dataset[Job] = {

    import spark.implicits._

    val start = events.filter(_.contains("SparkListenerJobStart")).
      map(string => parseJSON[SparkJobStart](string))
    val end = events.filter(_.contains("SparkListenerJobEnd")).
      map(string => parseJSON[SparkJobEnd](string))

    start.joinWith(end, start("Job$u0020ID") === end("Job$u0020ID")).
      map(tuple => {
        val jobId = tuple._1.`Job ID`
        val jobStartTime = tuple._1.`Submission$u0020Time`
        val jobEndTime = tuple._2.`Completion$u0020Time`
        val stages = tuple._1.`Stage$u0020IDs`
        val jobDuration = jobEndTime - jobStartTime
        val result = tuple._2.`Job$u0020Result`.`Result`
        Job(sparkApplication.applicationId,
          sparkApplication.applicationName,
          jobId,
          sparkApplication.applicationStartTime,
          sparkApplication.applicationEndTime,
          jobStartTime, jobEndTime, jobDuration,
          sparkApplication.applicationDuration, stages, result)
      })
  }

  /**
    * Parse stage complete events to return a dataset of {@link Stage}
    * @param spark
    * @param events
    * @param sparkJob
    * @return
    */
  def parseStages(spark: SparkSession,
                  events: Dataset[String],
                  sparkJob: Dataset[Job]): Dataset[Stage] = {

    import spark.implicits._

    val jobs = sparkJob.collect()
    events.filter(_.contains("SparkListenerStageCompleted")).
      map(string => parseJSON[SparkStageComplete](string)).
      map(stage => {
        val stageInfo = stage.`Stage$u0020Info`
        val stageId = stageInfo.`Stage$u0020ID`
        val job = jobs.filter(j => j.stages.contains(stageId)).last
        val jobId = job.jobId
        val attempt = stageInfo.`Stage$u0020Attempt$u0020ID`
        val stageName = stageInfo.`Stage$u0020Name`
        val details = stageInfo.`Details`
        val taskCount = stageInfo.`Number$u0020of$u0020Tasks`
        val rddCount = stageInfo.`RDD$u0020Info`.size
        val applicationStartTime = job.applicationStartTime
        val applicationEndTime = job.applicationEndTime
        val jobStartTime = job.jobStartTime
        val jobEndTime = job.jobEndTime
        val stageStartTime = stageInfo.`Submission$u0020Time`
        val stageEndTime = stageInfo.`Completion$u0020Time`
        val stageDuration = stageEndTime - stageStartTime
        val jobDuration = job.jobDuration
        val applicationDuration = job.applicationDuration
        Stage(job.applicationId, job.applicationName, jobId, stageId, stageName,
          attempt, details, taskCount, rddCount, applicationStartTime, applicationEndTime,
          jobStartTime, jobEndTime, stageStartTime, stageEndTime, stageDuration, jobDuration, applicationDuration)
      })
  }

  /**
    * Parse task end events to return a dataset of {@link Task}.
    * Note that the task event has a number of nested entities that are
    * introspected to extract relevant metrics.
    * @param spark
    * @param events
    * @param sparkStage
    * @return
    */
  def parseTasks(spark: SparkSession,
                 events: Dataset[String],
                 sparkStage: Dataset[Stage]): Dataset[Task] = {

    import spark.implicits._

    val tasks = events.filter(_.contains("SparkListenerTaskEnd")).
      map(string => parseJSON[SparkTaskEnd](string))

    val joined = tasks.joinWith(sparkStage,
      tasks("Stage$u0020ID") === sparkStage("stageId") &&
        tasks("Stage$u0020Attempt$u0020ID") === sparkStage("stageAttemptId"))
      joined.map(tuple => {
        val applicationId = tuple._2.applicationId
        val applicationName = tuple._2.applicationName
        val taskInfo = tuple._1.`Task$u0020Info`
        val taskMetrics = tuple._1.`Task$u0020Metrics`
        val accumulableMemory = taskInfo.`Accumulables`.
          filter(_.`Name` == "peakExecutionMemory").
          map(i => i.`Value`.toLong)
        val peakMemory = if (accumulableMemory.nonEmpty) accumulableMemory.head else 0
        val accumulableInputRows = taskInfo.`Accumulables`.
          filter(_.`Name` == "number of input rows").
          map(i => i.`Value`.toLong)
        val inputRows = if (accumulableInputRows.nonEmpty) accumulableInputRows.head else 0
        val accumulableOutputRows = taskInfo.`Accumulables`.
          filter(_.`Name` == "number of output rows").
          map(i => i.`Value`.toLong)
        val outputRows = if (accumulableOutputRows.nonEmpty) accumulableOutputRows.head else 0
        val jobId = tuple._2.jobId
        val stageId = tuple._2.stageId
        val stageAttemptId = tuple._2.stageAttemptId
        val taskId = taskInfo.`Task$u0020ID`
        val attempt = taskInfo.`Attempt`
        val executorId = taskInfo.`Executor$u0020ID`
        val host = taskInfo.`Host`
        val stageName = tuple._2.stageName
        val locality = taskInfo.`Locality`
        val speculative = taskInfo.`Speculative`
        val applicationStartTime = tuple._2.applicationStartTime
        val applicationEndTime = tuple._2.applicationEndTime
        val jobStartTime = tuple._2.jobStartTime
        val jobEndTime = tuple._2.jobEndTime
        val stageStartTime = tuple._2.stageStartTime
        val stageEndTime = tuple._2.stageEndTime
        val taskStartTime = taskInfo.`Launch$u0020Time`
        val taskEndTime = taskInfo.`Finish$u0020Time`
        val failed = taskInfo.`Failed`
        val taskDuration = taskEndTime - taskStartTime
        val stageDuration = tuple._2.stageDuration
        val jobDuration = tuple._2.jobDuration
        val applicationDuration = tuple._2.applicationDuration
        val gettingResultTime = taskInfo.`Getting$u0020Result$u0020Time`

        val gcTime = if (taskMetrics nonEmpty) taskMetrics.get.`JVM$u0020GC$u0020Time` else 0
        val resultSerializationTime = if (taskMetrics nonEmpty) taskMetrics.get.`Result$u0020Serialization$u0020Time` else 0
        val resultSize = if (taskMetrics nonEmpty) taskMetrics.get.`Result$u0020Size` else 0
        val memoryBytesSpilled = if (taskMetrics nonEmpty) taskMetrics.get.Memory$u0020Bytes$u0020Spilled else 0
        val diskBytesSpilled = if (taskMetrics nonEmpty) taskMetrics.get.`Disk$u0020Bytes$u0020Spilled` else 0

        val inputMetrics = if (taskMetrics nonEmpty) taskMetrics.get.`Input$u0020Metrics` else None
        val shuffleWriteMetrics = if (taskMetrics nonEmpty) taskMetrics.get.`Shuffle$u0020Write$u0020Metrics` else None
        val shuffleReadMetrics = if (taskMetrics nonEmpty) taskMetrics.get.`Shuffle$u0020Read$u0020Metrics` else None

        val dataReadMethod = if (inputMetrics nonEmpty) inputMetrics.get.Data$u0020Read$u0020Method else "n/a"
        val bytesRead = if (inputMetrics nonEmpty) inputMetrics.get.Bytes$u0020Read else 0L
        val recordsRead =
          inputMetrics match {
            case None => 0L
            case Some(_) => inputMetrics.get.`Records$u0020Read`}
        val shuffleBytesWritten =
          shuffleWriteMetrics match {
            case None => 0L
            case Some(_) => shuffleWriteMetrics.get.`Shuffle$u0020Bytes$u0020Written`}
        val shuffleRecordsWritten =
          shuffleWriteMetrics match {
            case None => 0L
            case Some(_) => shuffleWriteMetrics.get.`Shuffle$u0020Records$u0020Written`}
        val shuffleWriteTime =
          shuffleWriteMetrics match {
            case None => 0L
            case Some(_) => shuffleWriteMetrics.get.`Shuffle$u0020Write$u0020Time`}
        val remoteBlocksFetched =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Remote$u0020Blocks$u0020Fetched`}
        val localBlocksFetched =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Local$u0020Blocks$u0020Fetched`}
        val fetchWaitTime =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Fetch$u0020Wait$u0020Time`}
        val remoteBytesRead =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Remote$u0020Bytes$u0020Read`}
        val localBytesRead =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Local$u0020Bytes$u0020Read`}
        val totalRecordsRead =
          shuffleReadMetrics match {
            case None => 0L
            case Some(_) => shuffleReadMetrics.get.`Total$u0020Records$u0020Read`}
        Task(applicationId,
          applicationName,
          jobId,
          stageId,
          stageName,
          stageAttemptId,
          taskId,
          attempt,
          executorId,
          host,
          peakMemory,
          inputRows,
          outputRows,
          locality,
          speculative,
          applicationStartTime,
          applicationEndTime,
          jobStartTime,
          jobEndTime,
          stageStartTime,
          stageEndTime,
          taskStartTime,
          taskEndTime,
          failed,
          taskDuration,
          stageDuration,
          jobDuration,
          applicationDuration,
          gcTime,
          gettingResultTime,
          resultSerializationTime,
          resultSize,
          dataReadMethod,
          bytesRead,
          recordsRead,
          memoryBytesSpilled,
          diskBytesSpilled,
          shuffleBytesWritten,
          shuffleRecordsWritten,
          shuffleWriteTime,
          remoteBlocksFetched,
          localBlocksFetched,
          fetchWaitTime,
          remoteBytesRead,
          localBytesRead,
          totalRecordsRead
      )

    })
  }

}

/**
  * Executors are the long running JVMs (executing engines) used to execute the actual tasks of a spark application.
  * Note that a spark application contains one or more jobs, a job contains one or more stages and a stage has one or more tasks.
  * @param applicationId
  * @param applicationName
  * @param executorId
  * @param host
  * @param cores
  * @param startTime
  */
case class Executor(applicationId: String,
                    applicationName: String,
                    executorId: String,
                    host: String,
                    cores: Int,
                    startTime: Long)

private case class SparkExecutorAdded(`Timestamp`: Long,
                              `Executor ID`: String,
                              `Executor Info`: SparkExecutorInfo)

private case class SparkExecutorInfo(`Host`: String,
                             `Total Cores`: Int,
                             `Log Urls`: SparkLogUrls)

private case class SparkLogUrls(`stdout`: String,
                        `stderr`: String)

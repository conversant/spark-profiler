case class Job(applicationId: String,
               applicationName: String,
               jobId: Int,
               applicationStartTime: Long,
               applicationEndTime: Long,
               jobStartTime: Long,
               jobEndTime: Long,
               jobDuration: Long,
               applicationDuration: Long,
               stages: Seq[Int],
               result: String)

private case class SparkJobEnd(`Job ID`: Int,
                       `Job Result`: SparkJobResult,
                       `Completion Time`: Long)

private case class SparkJobResult(`Result`: String)

private case class SparkJobStart(`Job ID`: Int,
                         `Submission Time`: Long,
                         `Stage IDs`: Seq[Int])

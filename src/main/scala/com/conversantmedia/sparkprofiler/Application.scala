package com.conversantmedia.sparkprofiler

/**
  * Represents an instance of a spark application run/execution.
  * @param applicationId
  * @param applicationName
  * @param applicationStartTime
  * @param applicationEndTime
  * @param applicationDuration
  */
case class Application(applicationId: String,
                       applicationName: String,
                       applicationStartTime: Long,
                       applicationEndTime: Long,
                       applicationDuration: Long)


private case class  SparkApplicationEnd(Timestamp: Long)

private case class  SparkApplicationStart(`App Name`: String,
                                  `App ID`: String,
                                  `Timestamp`: Long,
                                  `User`: String)


package com.conversantmedia.sparkprofiler

case class Stage(applicationId: String,
                 applicationName: String,
                 jobId: Int,
                 stageId: Int,
                 stageName: String,
                 stageAttemptId: Int,
                 details: String,
                 taskCount: Int,
                 rddCount: Int,
                 applicationStartTime: Long,
                 applicationEndTime: Long,
                 jobStartTime: Long,
                 jobEndTime: Long,
                 stageStartTime: Long,
                 stageEndTime: Long,
                 stageDuration: Long,
                 jobDuration: Long,
                 applicationDuration: Long
                )

private case class StageInfo(`Stage ID`: Int,
                     `Stage Attempt ID`: Int,
                     `Stage Name`: String,
                     `Number of Tasks`: Int,
                     `RDD Info`: Seq[SparkRDD],
                     `Parent IDs`: Seq[Int],
                     `Details`: String,
                     `Submission Time`: Long,
                     `Completion Time`: Long,
                     `Accumulables`: Option[Seq[SparkAccumulable]]
                    )

private case class SparkRDD(`RDD ID`: Int,
                    `Name`: String,
                    `Storage Level`: StorageLevel,
                    `Number of Partitions`: Int,
                    `Memory Size`: Long,
                    `Disk Size`: Long)

private case class StorageLevel(`Use Disk`: Boolean,
                        `Use Memory`: Boolean,
                        `Use ExternalBlockStore`: Boolean,
                        `Deserialized`: Boolean,
                        `Replication`: Int
                       )

private case class SparkAccumulable(`ID`: Int,
                            `Name`: String,
                            `Update`: String,
                            `Value`: String,
                            `Internal`: Boolean)

private case class SparkStageComplete(`Stage Info`: StageInfo)

private case class SparkStageSubmitted(`Stage Info`: StageInfo)


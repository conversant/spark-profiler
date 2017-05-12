package com.conversantmedia.sparkprofiler

case class Task(applicationId: String,
                applicationName: String,
                jobId: Int,
                stageId: Int,
                stageName: String,
                stageAttemptId: Int,
                taskId: Int,
                taskAttemptId: Int,
                taskType: String,
                executorId: String,
                host: String,
                peakMemory: Long,
                inputRows: Long,
                outputRows: Long,
                locality: String,
                speculative: Boolean,
                applicationStartTime: Long,
                applicationEndTime: Long,
                jobStartTime: Long,
                jobEndTime: Long,
                stageStartTime: Long,
                stageEndTime: Long,
                taskStartTime: Long,
                taskEndTime: Long,
                failed: Boolean,
                taskDuration: Long,
                stageDuration: Long,
                jobDuration: Long,
                applicationDuration: Long,
                gcTime: Long,
                gettingResultTime: Long,
                resultSerializationTime: Long,
                resultSize: Long,
                dataReadMethod: String,
                bytesRead: Long,
                recordsRead: Long,
                memoryBytesSpilled: Long,
                diskBytesSpilled: Long,
                shuffleBytesWritten: Long,
                shuffleRecordsWritten: Long,
                shuffleWriteTime: Long,
                remoteBlocksFetched: Long,
                localBlocksFetched: Long,
                fetchWaitTime: Long,
                remoteBytesRead: Long,
                localBytesRead: Long,
                totalRecordsRead: Long
               )

private case class SparkTaskEnd(`Event`: String,
                        `Stage ID`: Int,
                        `Stage Attempt ID`: Int,
                        `Task Type`: String,
                        `Task End Reason`: SparkReason,
                        `Task Info`: TaskInfo,
                        `Task Metrics`: Option[TaskMetrics])

private case class SparkReason(`Reason`: String)

private case class TaskInfo(`Task ID`: Int,
                    `Index`: Int,
                    `Attempt`: Int,
                    `Launch Time`: Long,
                    `Executor ID`: String,
                    `Host`: String,
                    `Locality`: String,
                    `Speculative`: Boolean,
                    `Getting Result Time`: Int,
                    `Finish Time`: Long,
                    `Failed`: Boolean,
                    `Accumulables`: Seq[SparkAccumulable]
                   )

private case class TaskMetrics(`Host Name`: String,
                       `Executor Deserialize Time`: Long,
                       `Executor Run Time`: Long,
                       `Result Size`: Long,
                       `JVM GC Time`: Long,
                       `Result Serialization Time`: Long,
                       `Memory Bytes Spilled`: Long,
                       `Disk Bytes Spilled`: Long,
                       `Input Metrics`: Option[InputMetrics],
                       `Shuffle Write Metrics`: Option[SparkShuffleWriteMetrics],
                       `Shuffle Read Metrics`: Option[SparkShuffleReadMetrics]
                      )

private case class InputMetrics(`Data Read Method`: String,
                        `Bytes Read`: Long,
                        `Records Read`: Long)

private case class SparkShuffleWriteMetrics(`Shuffle Bytes Written`: Long,
                                    `Shuffle Write Time`: Long,
                                    `Shuffle Records Written`: Long
                                   )

private case class SparkShuffleReadMetrics(`Remote Blocks Fetched`: Long,
                                   `Local Blocks Fetched`: Long,
                                   `Fetch Wait Time`: Long,
                                   `Remote Bytes Read`: Long,
                                   `Local Bytes Read`: Long,
                                   `Total Records Read`: Long
                                  )


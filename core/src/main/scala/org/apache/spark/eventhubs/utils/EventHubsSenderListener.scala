package org.apache.spark.eventhubs.utils

import com.microsoft.azure.eventhubs.EventDataBatch

trait EventHubsSenderListener extends Serializable {

  def onBatchSendSuccess(messageCount: Int, messageSizeInBytes: Int, sendElapsedTimeInNanos: Long, retryTimes: Int)

  def onBatchSendFail(exception: Throwable)

  def onWriterOpen(partitionId: Long, version: Long)

  def onWriterClose(totalMessageCount: Int,
                    totalMessageSizeInBytes: Int,
                    totalRetryTimes: Int,
                    endToEndElapsedTimeInNanos: Long,
                    totalBatches: Int)

  def getConstructorParameters: Seq[String]

}

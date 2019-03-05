package org.apache.spark.eventhubs.utils

import com.microsoft.azure.eventhubs.EventDataBatch

trait EventHubsSenderListener extends Serializable {

  def onBatchSendSuccess(eventDataBatch: EventDataBatch, sendElapsedTimeInNanos: Long, retryCount: Int)

  def onBatchSendFail(exception: Throwable)

  def onWriterOpen(partitionId: Long, version: Long)

  def onWriterClose(totalMessageCount: Int, totalMessageSizeInBytes: Int, endToEndElapsedTimeInNanos: Long)

  def getConstructorParameters: Seq[String]

}

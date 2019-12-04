package org.apache.spark.eventhubs.utils

trait EventHubsSenderListener extends Serializable {

  def onBatchSendSuccess(messageCount: Int, messageSizeInBytes: Int, sendElapsedTimeInNanos: Long)

  def onBatchSendFail(exception: Throwable)

  def onWriterOpen(partitionId: Long, version: Long)

  def onWriterClose(totalMessageCount: Int,
                    totalMessageSizeInBytes: Int,
                    endToEndElapsedTimeInNanos: Long,
                    totalBatches: Option[Int])

  def getConstructorParameters: Seq[String]

}

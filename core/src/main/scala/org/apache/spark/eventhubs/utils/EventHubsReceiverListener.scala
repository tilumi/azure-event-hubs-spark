package org.apache.spark.eventhubs.utils

import com.microsoft.azure.eventhubs.{EventData, EventDataBatch}
import org.apache.spark.eventhubs.{NameAndPartition, SequenceNumber}

trait EventHubsReceiverListener extends Serializable {

  def onBatchReceiveSuccess(nAndP: NameAndPartition, elapsedTime: Long, batchSize: Int, receivedBytes: Long): Unit

  def onBatchReceiveSkip(nAndP: NameAndPartition, requestSeqNo: SequenceNumber, batchSize: Int): Unit

  def onReceiveFirstEvent(firstEvent: EventData): Unit

  def getConstructorParameters: Seq[String]

  def onBatchSendSuccess(eventDataBatch: EventDataBatch, sendElapsedTimeInNanos: Long, retryCount: Int)

  def onBatchSendFail(exception: Throwable)

  def onWriterOpen(partitionId: Long, version: Long)

  def onWriterClose(totalMessageCount: Int, totalMessageSizeInBytes: Int, endToEndElapsedTimeInNanos: Long)

}

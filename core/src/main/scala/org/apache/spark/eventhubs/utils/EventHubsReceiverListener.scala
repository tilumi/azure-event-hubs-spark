package org.apache.spark.eventhubs.utils

import org.apache.spark.eventhubs.{NameAndPartition, SequenceNumber}

trait EventHubsReceiverListener extends Serializable {

  def onBatchReceiveSuccess(nAndP: NameAndPartition, elapsedTime: Long, batchSize: Int): Unit

  def onBatchReceiveSkip(nAndP: NameAndPartition, requestSeqNo: SequenceNumber, batchSize: Int): Unit

}
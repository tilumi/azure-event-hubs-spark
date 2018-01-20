/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.eventhubscommon.rdd

import java.time.Instant
import java.util.Collections

import com.microsoft.azure.eventhubs.EventData
import org.apache.hadoop.conf.Configuration
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.eventhubscommon.EventHubNameAndPartition
import org.apache.spark.eventhubscommon.client.EventHubsClientWrapper
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubscommon.progress.ProgressWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

private class EventHubRDDPartition(val sparkPartitionId: Int,
                                   val eventHubNameAndPartitionID: EventHubNameAndPartition,
                                   val fromOffset: Long,
                                   val fromSeq: Long,
                                   val untilSeq: Long,
                                   val offsetType: EventHubsOffsetType)
    extends Partition {

  override def index: Int = sparkPartitionId
}

private[spark] class EventHubsRDD(sc: SparkContext,
                                  eventHubsParamsMap: Map[String, Map[String, String]],
                                  val offsetRanges: List[OffsetRange],
                                  batchTime: Long,
                                  offsetParams: OffsetStoreParams,
                                  eventHubReceiverCreator: (Map[String, String],
                                                            Int,
                                                            Long,
                                                            EventHubsOffsetType,
                                                            Int) => EventHubsClientWrapper,
                                  accumulators: Seq[LongAccumulator])
    extends RDD[EventData](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map {
      case (offsetRange, index) =>
        new EventHubRDDPartition(index,
                                 offsetRange.eventHubNameAndPartition,
                                 offsetRange.fromOffset,
                                 offsetRange.fromSeq,
                                 offsetRange.untilSeq,
                                 offsetRange.offsetType)
    }.toArray
  }

  private def processFullyConsumedPartition(ehRDDPartition: EventHubRDDPartition,
                                            progressWriter: ProgressWriter): Iterator[EventData] = {
    logInfo(s"No new data in ${ehRDDPartition.eventHubNameAndPartitionID} at $batchTime")
    val fromOffset = ehRDDPartition.fromOffset
    progressWriter.write(batchTime, ehRDDPartition.fromOffset, ehRDDPartition.fromSeq, -1L)
    logInfo(
      s"write offset $fromOffset, sequence number" +
        s" ${ehRDDPartition.fromSeq} for EventHub" +
        s" ${ehRDDPartition.eventHubNameAndPartitionID} at $batchTime")
    Iterator()
  }

  private def extractOffsetAndSeqToWrite(lastEventOption: Option[EventData],
                                         eventHubReceiver: EventHubsClientWrapper,
                                         ehRDDPartition: EventHubRDDPartition): (Long, Long, Long) = {
    lastEventOption match {
      case Some(lastEvent) =>
        (lastEvent.getSystemProperties.getOffset.toLong,
        lastEvent.getSystemProperties.getSequenceNumber,
        lastEvent.getSystemProperties.getEnqueuedTime.toEpochMilli)
      case _ =>
        val partitionInfo = eventHubReceiver.eventhubsClient
          .getPartitionRuntimeInformation(
            ehRDDPartition.eventHubNameAndPartitionID.partitionId.toString)
          .get()
        (partitionInfo.getLastEnqueuedOffset.toLong,
         partitionInfo.getLastEnqueuedSequenceNumber,
         partitionInfo.getLastEnqueuedTimeUtc.toEpochMilli)
    }
  }

  private def retrieveDataFromPartition(ehRDDPartition: EventHubRDDPartition,
                                        progressWriter: ProgressWriter,
                                        context: TaskContext): Iterator[EventData] = {
    val fromOffset = ehRDDPartition.fromOffset
    val fromSeq = ehRDDPartition.fromSeq
    val untilSeq = ehRDDPartition.untilSeq
    val maxRate = (untilSeq - fromSeq).toInt
    logInfo(
      s"${ehRDDPartition.eventHubNameAndPartitionID}" +
        s" expected rate $maxRate, fromSeq $fromSeq (exclusive) untilSeq" +
        s" $untilSeq (inclusive) at $batchTime")
    new Iterator[EventData] {
      val eventHubParameters = eventHubsParamsMap(
        ehRDDPartition.eventHubNameAndPartitionID.eventHubName)
      val eventHubReceiver = eventHubReceiverCreator(
        eventHubParameters,
        ehRDDPartition.eventHubNameAndPartitionID.partitionId,
        fromOffset,
        ehRDDPartition.offsetType,
        maxRate)
      val receivingTrace = new ListBuffer[Int]
      var receivedSize = 0
      var cnt = 0
      var lastEventOption:Option[EventData] = None
      var receivedEventsItr:Iterator[EventData] = _
      var totalReceivingTime = 0L
      val receivingStartTimeAccumulator = accumulators(1)
      val receivingEndTimeAccumulator = accumulators(2)
      var receivingStartTime = -1L
      var receivingEndTime = -1L

      override def hasNext: Boolean = {
        val isReachedUntil = lastEventOption match {
          case Some(lastEvent) =>
            lastEvent.getSystemProperties.getSequenceNumber >= ehRDDPartition.untilSeq ||
            lastEvent.getSystemProperties.getEnqueuedTime.isAfter(Instant.ofEpochMilli(batchTime))
          case _ =>
            false
        }
        if (isReachedUntil) {
          cleanup()
          return false
        }
        def time[R](block: => R): R = {
          if (receivingStartTime < 0) {
            receivingStartTime = System.currentTimeMillis()
          }
          val t0 = System.currentTimeMillis()
          val result = block    // call-by-name
          val t1 = System.currentTimeMillis()
          totalReceivingTime += (t1 - t0)
          receivingEndTime = System.currentTimeMillis()
          result
        }
        var hasNext = receivedEventsItr != null && receivedEventsItr.hasNext
        if (!hasNext) {
          if (cnt > maxRate * 2) {
            throw new Exception(
              s"${ehRDDPartition.eventHubNameAndPartitionID} cannot return data, the trace is" +
                s" ${receivingTrace.toList}")
          }
          receivedEventsItr = time(Try(eventHubReceiver.receive(Math.max(1, maxRate - receivedSize)))) match {
            case Success(iterable) =>
              if (iterable != null) {
                receivingTrace += iterable.size
                receivedSize += receivingTrace.last
                cnt += 1
                iterable.iterator
              } else {
                logInfo("no more messages")
                null
              }
            case Failure(exception) =>
              exception.printStackTrace()
              null
          }
          hasNext = receivedEventsItr != null && receivedEventsItr.hasNext
        }
        if (!hasNext) {
          cleanup()
        }
        hasNext
      }

      override def next(): EventData = {
        val eventData = receivedEventsItr.next()
        lastEventOption = Some(eventData)
        eventData
      }

      private def cleanup(): Unit = {
        try {
          val (offsetToWrite, seqToWrite, instantToWrite) =
            extractOffsetAndSeqToWrite(lastEventOption, eventHubReceiver, ehRDDPartition)
          progressWriter.write(batchTime, offsetToWrite, seqToWrite, instantToWrite)
          logInfo(
            s"write offset $offsetToWrite, sequence number $seqToWrite for EventHub" +
              s" ${ehRDDPartition.eventHubNameAndPartitionID} at $batchTime")
          val throughput = receivedSize / (totalReceivingTime / 1000D)
          logInfo(s"Received $receivedSize records in ${totalReceivingTime / 1000D} seconds, " +
            s"throughput: $throughput")
          val inputElapsedTimeAccumulator = accumulators.head
          inputElapsedTimeAccumulator.add(totalReceivingTime)
          receivingStartTimeAccumulator.add(receivingStartTime)
          receivingEndTimeAccumulator.add(receivingEndTime)
        } finally {
          eventHubReceiver.close()
        }
      }
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[EventData] = {
    val ehRDDPartition = split.asInstanceOf[EventHubRDDPartition]
    logInfo(s"offsetType: ${ehRDDPartition.offsetType}, offset: ${ehRDDPartition.fromOffset}")
    val progressWriter = new ProgressWriter(
      offsetParams.streamId,
      offsetParams.uid,
      ehRDDPartition.eventHubNameAndPartitionID,
      batchTime,
      new Configuration(),
      offsetParams.checkpointDir,
      offsetParams.subDirs: _*
    )
    if (ehRDDPartition.fromSeq >= ehRDDPartition.untilSeq) {
      processFullyConsumedPartition(ehRDDPartition, progressWriter)
    } else {
      retrieveDataFromPartition(ehRDDPartition, progressWriter, context: TaskContext)
    }
  }
}

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

package org.apache.spark.eventhubs.client

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.microsoft.azure.eventhubs._
import org.apache.spark.SparkEnv
import org.apache.spark.eventhubs.utils.EventHubsReceiverListener
import org.apache.spark.eventhubs.utils.RetryUtils.{after, retryJava, retryNotNull}
import org.apache.spark.eventhubs.{DefaultConsumerGroup, EventHubsConf, EventHubsUtils, NameAndPartition, SequenceNumber}
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Awaitable, Future}

private[spark] trait CachedReceiver {
  private[eventhubs] def receive(ehConf: EventHubsConf,
                                 nAndP: NameAndPartition,
                                 requestSeqNo: SequenceNumber,
                                 batchSize: Int,
                                   eventHubsReceiverListener: Option[EventHubsReceiverListener] = None): Iterator[EventData]
}

/**
  * An Event Hubs receiver instance that is cached on a Spark Executor to be
  * reused across batches. In Structured Streaming and Spark Streaming,
  * partitions are generated on the same executors across batches, so that
  * receivers can be cached are reused for maximum efficiency. Receiver caching
  * allows the underlying [[PartitionReceiver]]s to prefetch [[EventData]] from
  * the service before DataFrames or RDDs are generated by Spark.
  *
  * This class creates and maintains an AMQP link with the Event Hubs service.
  * On creation, an [[EventHubClient]] is borrowed from the [[ClientConnectionPool]].
  * Then a [[PartitionReceiver]] is created on top of the borrowed connection with the
  * [[NameAndPartition]].
  *
  * @param ehConf the [[EventHubsConf]] which contains the connection string used to connect to Event Hubs
  * @param nAndP  the Event Hub name and partition that the receiver is connected to.
  */
private[client] class CachedEventHubsReceiver private (ehConf: EventHubsConf,
                                                       nAndP: NameAndPartition,
                                                       startSeqNo: SequenceNumber)
  extends Logging {

  type AwaitTimeoutException = java.util.concurrent.TimeoutException

  import org.apache.spark.eventhubs._

  private lazy val client: EventHubClient = ClientConnectionPool.borrowClient(ehConf)

  private var receiver: PartitionReceiver = createReceiver(startSeqNo)

  private def createReceiver(seqNo: SequenceNumber): PartitionReceiver = {
    val taskId = EventHubsUtils.getTaskId
    logInfo(
      s"(TID $taskId) creating receiver for Event Hub ${nAndP.ehName} on partition ${nAndP.partitionId}. seqNo: $seqNo")
    val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
    val receiverOptions = new ReceiverOptions
    receiverOptions.setReceiverRuntimeMetricEnabled(true)
    receiverOptions.setPrefetchCount(ehConf.prefetchCount.getOrElse(DefaultPrefetchCount))
    receiverOptions.setIdentifier(s"spark-${SparkEnv.get.executorId}-$taskId")
    val consumer = retryJava(
      EventHubsUtils.createReceiverInner(client,
        ehConf.useExclusiveReceiver,
        consumerGroup,
        nAndP.partitionId.toString,
        EventPosition.fromSequenceNumber(seqNo).convert,
        receiverOptions),
      "CachedReceiver creation."
    )
    Await.result(consumer, ehConf.internalOperationTimeout)
  }

  private def lastReceivedOffset(): Future[Long] = {
    if (receiver.getEventPosition.getSequenceNumber != null) {
      Future.successful(receiver.getEventPosition.getSequenceNumber)
    } else {
      Future.successful(-1)
    }
  }

  private def receiveOne(timeout: Duration, msg: String): Future[Iterable[EventData]] = {
    def receiveOneWithRetry(timeout: Duration,
                            msg: String,
                            retryCount: Int): Future[Iterable[EventData]] = {
      if (!receiver.getIsOpen && retryCount < RetryCount) {
        val taskId = EventHubsUtils.getTaskId
        logInfo(
          s"(TID $taskId) receiver is not opened yet. Will retry.. $nAndP, consumer group: ${ehConf.consumerGroup
            .getOrElse(DefaultConsumerGroup)}")

        after(WaitInterval.milliseconds)(receiveOneWithRetry(timeout, msg, retryCount + 1))
      }

      receiver.setReceiveTimeout(timeout)
      retryNotNull(receiver.receive(1), msg).map(
        _.asScala
      )
    }
    receiveOneWithRetry(timeout, msg, retryCount = 0)
  }

  private def closeReceiver(): Future[Void] = {
    retryJava(receiver.close(), "closing a receiver")
  }

  private def recreateReceiver(seqNo: SequenceNumber): Unit = {
    val taskId = EventHubsUtils.getTaskId
    val startTimeNs = System.nanoTime()
    def elapsedTimeNs = System.nanoTime() - startTimeNs

    Await.result(closeReceiver(), ehConf.internalOperationTimeout)
    receiver = createReceiver(seqNo)

    val elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs)
    logInfo(s"(TID $taskId) Finished recreating a receiver for $nAndP, ${ehConf.consumerGroup
      .getOrElse(DefaultConsumerGroup)}: $elapsedTimeMs ms")
  }

  private def checkCursor(requestSeqNo: SequenceNumber): Future[Iterable[EventData]] = {
    val taskId = EventHubsUtils.getTaskId

    val lastReceivedSeqNo =
      Await.result(lastReceivedOffset(), ehConf.internalOperationTimeout)

    if (lastReceivedSeqNo > -1 && lastReceivedSeqNo + 1 != requestSeqNo) {
      logInfo(s"(TID $taskId) checkCursor. Recreating a receiver for $nAndP, ${ehConf.consumerGroup.getOrElse(
        DefaultConsumerGroup)}. requestSeqNo: $requestSeqNo, lastReceivedSeqNo: $lastReceivedSeqNo")

      recreateReceiver(requestSeqNo)
    }

    val event = awaitReceiveMessage(
      receiveOne(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout), "checkCursor initial"),
      requestSeqNo)
    val receivedSeqNo = event.head.getSystemProperties.getSequenceNumber

    if (receivedSeqNo != requestSeqNo) {
      // This can happen in two cases:
      // 1) Your desired event is still in the service, but the receiver
      //    cursor is in the wrong spot.
      // 2) Your desired event has expired from the service.
      // First, we'll check for case (1).
      logInfo(
        s"(TID $taskId) checkCursor. Recreating a receiver for $nAndP, ${ehConf.consumerGroup.getOrElse(
          DefaultConsumerGroup)}. requestSeqNo: $requestSeqNo, receivedSeqNo: $receivedSeqNo")
      recreateReceiver(requestSeqNo)
      val movedEvent = awaitReceiveMessage(
        receiveOne(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout), "checkCursor move"),
        requestSeqNo)
      val movedSeqNo = movedEvent.head.getSystemProperties.getSequenceNumber
      if (movedSeqNo != requestSeqNo) {
        // The event still isn't present. It must be (2).
        val info = Await.result(
          retryJava(client.getPartitionRuntimeInformation(nAndP.partitionId.toString),
            "partitionRuntime"),
          ehConf.internalOperationTimeout)

        if (requestSeqNo < info.getBeginSequenceNumber &&
          movedSeqNo == info.getBeginSequenceNumber) {
          Future {
            movedEvent
          }
        } else {
          val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
          throw new IllegalStateException(
            s"In partition ${info.getPartitionId} of ${info.getEventHubPath}, with consumer group $consumerGroup, " +
              s"request seqNo $requestSeqNo is less than the received seqNo $receivedSeqNo. The earliest seqNo is " +
              s"${info.getBeginSequenceNumber} and the last seqNo is ${info.getLastEnqueuedSequenceNumber}")
        }
      } else {
        Future {
          movedEvent
        }
      }
    } else {
      Future {
        event
      }
    }
  }

  private def receive(requestSeqNo: SequenceNumber, batchSize: Int, eventHubsReceiverListener: Option[EventHubsReceiverListener] = None): Iterator[EventData] = {
    val taskId = EventHubsUtils.getTaskId
    val startTimeNs = System.nanoTime()
    def elapsedTimeNs = System.nanoTime() - startTimeNs

    // Retrieve the events. First, we get the first event in the batch.
    // Then, if the succeeds, we collect the rest of the data.
    val first = Await.result(checkCursor(requestSeqNo), ehConf.internalOperationTimeout)
    eventHubsReceiverListener.foreach(_.onReceiveFirstEvent(nAndP, first.head))
    val firstSeqNo = first.head.getSystemProperties.getSequenceNumber
    val newBatchSize = (requestSeqNo + batchSize - firstSeqNo).toInt

    if (newBatchSize <= 0) {
      return Iterator.empty
    }

    val theRest = for { i <- 1 until newBatchSize } yield
      awaitReceiveMessage(receiveOne(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout),
        s"receive; $nAndP; seqNo: ${requestSeqNo + i}"),
        requestSeqNo)
    // Combine and sort the data.
    val combined = first ++ theRest.flatten
    val sorted = combined.toSeq
      .sortWith((e1, e2) =>
        e1.getSystemProperties.getSequenceNumber < e2.getSystemProperties.getSequenceNumber)
      .iterator

    val (result, validate) = sorted.duplicate
    val (count, size) = validate.map(event => (1, event.getBytes.length.toLong)).reduce{
      (value1, value2) =>{
        (value1._1 + value2._1, value1._2 + value2._2)
      }
    }
    assert(count == newBatchSize)

    val elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs)
    logInfo(s"(TID $taskId) Finished receiving for $nAndP, consumer group: ${ehConf.consumerGroup
      .getOrElse(DefaultConsumerGroup)}, batchSize: $batchSize, elapsed time: $elapsedTimeMs ms")
    eventHubsReceiverListener.foreach(_.onBatchReceiveSuccess(nAndP, elapsedTimeMs, count,size))
    result
  }

  private def awaitReceiveMessage[T](awaitable: Awaitable[T], requestSeqNo: SequenceNumber): T = {
    val taskId = EventHubsUtils.getTaskId

    try {
      Await.result(awaitable, ehConf.internalOperationTimeout)
    } catch {
      case e: AwaitTimeoutException =>
        logError(
          s"(TID $taskId) awaitReceiveMessage call failed with timeout. Event Hub $nAndP, ConsumerGroup ${ehConf.consumerGroup
            .getOrElse(DefaultConsumerGroup)}. requestSeqNo: $requestSeqNo")

        recreateReceiver(requestSeqNo)
        throw e
    }
  }
}

/**
  * A companion object to the [[CachedEventHubsReceiver]]. This companion object
  * serves as a singleton which carries all the cached receivers on a given
  * Spark executor.
  */
private[spark] object CachedEventHubsReceiver extends CachedReceiver with Logging {

  type MutableMap[A, B] = scala.collection.mutable.HashMap[A, B]

  private[this] val receivers = new MutableMap[String, CachedEventHubsReceiver]()

  private def key(ehConf: EventHubsConf, nAndP: NameAndPartition): String = {
    (ehConf.connectionString + ehConf.consumerGroup + nAndP.partitionId).toLowerCase
  }

  private[eventhubs] override def receive(ehConf: EventHubsConf,
                                          nAndP: NameAndPartition,
                                          requestSeqNo: SequenceNumber,
                                          batchSize: Int,
                                          eventHubsReceiverListener: Option[EventHubsReceiverListener] = None): Iterator[EventData] = {
    val taskId = EventHubsUtils.getTaskId

    logInfo(s"(TID $taskId) EventHubsCachedReceiver look up. For $nAndP, ${ehConf.consumerGroup
      .getOrElse(DefaultConsumerGroup)}. requestSeqNo: $requestSeqNo, batchSize: $batchSize")
    var receiver: CachedEventHubsReceiver = null
    receivers.synchronized {
      receiver = receivers.getOrElseUpdate(key(ehConf, nAndP), {
        CachedEventHubsReceiver(ehConf, nAndP, requestSeqNo)
      })
    }
    receiver.receive(requestSeqNo, batchSize, eventHubsReceiverListener)
  }

  def apply(ehConf: EventHubsConf,
            nAndP: NameAndPartition,
            startSeqNo: SequenceNumber): CachedEventHubsReceiver = {
    new CachedEventHubsReceiver(ehConf, nAndP, startSeqNo)
  }
}

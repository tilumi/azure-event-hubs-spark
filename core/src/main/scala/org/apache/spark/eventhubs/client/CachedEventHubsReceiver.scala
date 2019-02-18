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

import com.microsoft.azure.eventhubs._
import org.apache.spark.eventhubs.utils.EventHubsReceiverListener
import org.apache.spark.eventhubs.utils.RetryUtils.{retryJava, retryNotNull}
import org.apache.spark.eventhubs.{EventHubsConf, NameAndPartition, SequenceNumber}
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkEnv, TaskContext}

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Awaitable, Future, duration}

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

  private var receiver: Future[PartitionReceiver] = createReceiver(startSeqNo)

  private def createReceiver(seqNo: SequenceNumber): Future[PartitionReceiver] = {
    logInfo(
      s"creating receiver for Event Hub ${nAndP.ehName} on partition ${nAndP.partitionId}. seqNo: $seqNo")
    val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
    val receiverOptions = new ReceiverOptions
    receiverOptions.setReceiverRuntimeMetricEnabled(true)
    receiverOptions.setPrefetchCount(ehConf.prefetchCount.getOrElse(DefaultPrefetchCount))
    receiverOptions.setIdentifier(
      s"spark-${SparkEnv.get.executorId}-${TaskContext.get.taskAttemptId}")
    val epochReceiver = retryJava(
      client.createEpochReceiver(consumerGroup,
                                 nAndP.partitionId.toString,
                                 EventPosition.fromSequenceNumber(seqNo).convert,
                                 DefaultEpoch,
                                 receiverOptions),
      "CachedReceiver creation."
    )
    epochReceiver
  }

  private def lastReceivedOffset(): Future[Long] = {
    receiver
      .flatMap { r =>
        if (r.getEventPosition.getSequenceNumber != null) {
          Future.successful(r.getEventPosition.getSequenceNumber)
        } else {
          Future.successful(-1)
        }
      }
  }

  private def receiveOne(timeout: Duration, msg: String): Future[Iterable[EventData]] = {
    receive(timeout, 1, msg)
  }

  private def receive(timeout: Duration, maxEventCount: Int, msg: String): Future[Iterable[EventData]] = {
    receiver
      .flatMap { r =>
        r.setReceiveTimeout(timeout)
        retryNotNull(r.receive(maxEventCount), msg)
      }
      .map {
        _.asScala
      }
  }

  private def checkCursor(requestSeqNo: SequenceNumber): Future[Iterable[EventData]] = {
    val lastReceivedSeqNo =
      Await.result(lastReceivedOffset(),
        ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout) match { case timeout => FiniteDuration(timeout.toMillis, duration.MILLISECONDS) }
      )

    if (lastReceivedSeqNo > -1 && lastReceivedSeqNo + 1 != requestSeqNo) {
      logInfo(
        s"checkCursor. Recreating a receiver for $nAndP, ${ehConf.consumerGroup}. requestSeqNo: $requestSeqNo, lastReceivedSeqNo: $lastReceivedSeqNo")
      closeReceiver()
      receiver = createReceiver(requestSeqNo)
    }

    val event = awaitReceiveMessage(
      receiveOne(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout), "checkCursor initial"))
    val receivedSeqNo = event.head.getSystemProperties.getSequenceNumber

    if (receivedSeqNo != requestSeqNo) {
      // This can happen in two cases:
      // 1) Your desired event is still in the service, but the receiver
      //    cursor is in the wrong spot.
      // 2) Your desired event has expired from the service.
      // First, we'll check for case (1).
      logInfo(
        s"checkCursor. Recreating a receiver for $nAndP, ${ehConf.consumerGroup}. requestSeqNo: $requestSeqNo, receivedSeqNo: $receivedSeqNo")
      closeReceiver()
      receiver = createReceiver(requestSeqNo)
      val movedEvent = awaitReceiveMessage(
        receiveOne(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout), "checkCursor move"))
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

  private def closeReceiver(): Unit = {
    if (receiver != null) {
      Try(Await.ready(receiver.map(_.closeSync()), ehConf.internalOperationTimeout)) match {
        case Success(_) => logInfo("Receiver close succeed")
        case Failure(exception) => logWarning(s"Receiver close failed: ${exception.getMessage}")
      }
    }
  }

  private def receive(requestSeqNo: SequenceNumber,
                      batchSize: Int,
                      eventHubsReceiverListener: Option[EventHubsReceiverListener]): Iterator[EventData] = {
    val retryCount = ehConf.receiveRetryTimes.getOrElse(DefaultReceiveRetryTimes)
    var retried = 0
    var finalResult: Option[Iterator[EventData]] = None
    while (retried < retryCount && finalResult.isEmpty) {
      retried += 1
      Try {
        val start = System.currentTimeMillis()
        // Retrieve the events. First, we get the first event in the batch.
        // Then, if the succeeds, we collect the rest of the data.
        val first = Await.result(checkCursor(requestSeqNo), ehConf.internalOperationTimeout)
        val firstSeqNo = first.head.getSystemProperties.getSequenceNumber
        val newBatchSize = (requestSeqNo + batchSize - firstSeqNo).toInt

        if (newBatchSize <= 0) {
          return Iterator.empty
        }
        val theRest = (1 until newBatchSize).grouped(500).flatMap(group => {
            awaitReceiveMessage(receive(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout), group.size,
              s"receive; $nAndP; seqNo: [${requestSeqNo + group.head}, ${requestSeqNo + group.last}]"))
        })
        // Combine and sort the data.
        val combined = first ++ theRest
        val sorted = combined.toSeq
          .sortWith((e1, e2) =>
            e1.getSystemProperties.getSequenceNumber < e2.getSystemProperties.getSequenceNumber)
          .iterator

        val (result, validate) = sorted.duplicate
        assert(validate.size == newBatchSize)
        finalResult = Some(result)
        eventHubsReceiverListener.foreach(listener => {
          val receivedBytes = result.map(_.getBytes.size.toLong).sum
          listener.onBatchReceiveSuccess(nAndP, System.currentTimeMillis() - start, batchSize, receivedBytes)
        })
      } match {
        case Success(_) =>
        case Failure(exception) =>
          logWarning("Receive failure", exception)
          logInfo(
            s"Receive failure. Recreating a receiver for $nAndP, ${ehConf.consumerGroup}. requestSeqNo: $requestSeqNo")
          closeReceiver()
          receiver = createReceiver(requestSeqNo)
      }
    }
    finalResult.getOrElse({
      logWarning(
        s"Abandon the partition: " +
          s"${ConnectionStringBuilder(ehConf.connectionString).getEndpoint.getHost}-${nAndP.ehName}:${nAndP.partitionId}, " +
          s"requestSeqNo: $requestSeqNo, batchSize: $batchSize"
      )
      eventHubsReceiverListener.foreach(listener => {
        listener.onBatchReceiveSkip(nAndP, requestSeqNo, batchSize)
      })
      Seq.empty[EventData].iterator
    })
  }

  private def awaitReceiveMessage[T](awaitable: Awaitable[T]): T = {
    try {
      Await.result(awaitable, ehConf.internalOperationTimeout)
    } catch {
      case e: AwaitTimeoutException =>
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
    logInfo(
      s"EventHubsCachedReceiver look up. For $nAndP, ${ehConf.consumerGroup}. requestSeqNo: $requestSeqNo, batchSize: $batchSize")
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


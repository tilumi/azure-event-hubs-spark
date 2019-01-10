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

import com.microsoft.azure.eventhubs._
import org.apache.spark.eventhubs.utils.RetryUtils.{retryJava, retryNotNull}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.eventhubs.{EventHubsConf, NameAndPartition, SequenceNumber}
import org.apache.spark.internal.Logging

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

private[spark] trait CachedReceiver {
  private[eventhubs] def receive(ehConf: EventHubsConf,
                                 nAndP: NameAndPartition,
                                 requestSeqNo: SequenceNumber,
                                 batchSize: Int,
                                 receiveTimeoutHandler: Option[(NameAndPartition, SequenceNumber, Int) => Unit] = None): Iterator[EventData]
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
 * @param nAndP the Event Hub name and partition that the receiver is connected to.
 */
private[client] class CachedEventHubsReceiver private (ehConf: EventHubsConf,
                                                       nAndP: NameAndPartition,
                                                       startSeqNo: SequenceNumber)
    extends Logging {

  import org.apache.spark.eventhubs._

  private lazy val client: EventHubClient = ClientConnectionPool.borrowClient(ehConf)

  private var receiver: Future[PartitionReceiver] = createReceiver(startSeqNo)

  private def createReceiver(seqNo: SequenceNumber): Future[PartitionReceiver] = {
    logInfo(s"creating receiver for Event Hub ${nAndP.ehName} on partition ${nAndP.partitionId}")
    val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
    val receiverOptions = new ReceiverOptions
    receiverOptions.setReceiverRuntimeMetricEnabled(false)
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
    epochReceiver.onComplete {
      case Success(x) => x.setPrefetchCount(DefaultPrefetchCount)
      case _          =>
    }
    epochReceiver
  }

  private def receiveOne(msg: String): Future[Iterable[EventData]] = {
    receiver
      .flatMap { r =>
        retryNotNull(r.receive(1), msg)
      }
      .map { _.asScala }
  }

  private def checkCursor(requestSeqNo: SequenceNumber): Future[Iterable[EventData]] = {
    val event = Await.result(receiveOne("checkCursor initial"), ehConf.receiverTimeout.map{timeout => Duration.fromNanos(timeout.toNanos)}.getOrElse(DefaultReceiveTimeout))
    val receivedSeqNo = event.head.getSystemProperties.getSequenceNumber

    if (receivedSeqNo != requestSeqNo) {
      // This can happen in two cases:
      // 1) Your desired event is still in the service, but the receiver
      //    cursor is in the wrong spot.
      // 2) Your desired event has expired from the service.
      // First, we'll check for case (1).
      receiver = createReceiver(requestSeqNo)
      val movedEvent = Await.result(receiveOne("checkCursor move"), ehConf.receiverTimeout.map{timeout => Duration.fromNanos(timeout.toNanos)}.getOrElse(DefaultReceiveTimeout))
      val movedSeqNo = movedEvent.head.getSystemProperties.getSequenceNumber
      if (movedSeqNo != requestSeqNo) {
        // The event still isn't present. It must be (2).
        val info = Await.result(
          retryJava(client.getPartitionRuntimeInformation(nAndP.partitionId.toString),
                    "partitionRuntime"),
          ehConf.receiverTimeout.map{timeout => Duration.fromNanos(timeout.toNanos)}.getOrElse(DefaultReceiveTimeout))
        val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
        throw new IllegalStateException(
          s"In partition ${info.getPartitionId} of ${info.getEventHubPath}, with consumer group $consumerGroup, " +
            s"request seqNo $requestSeqNo is less than the received seqNo $receivedSeqNo. The earliest seqNo is " +
            s"${info.getBeginSequenceNumber} and the last seqNo is ${info.getLastEnqueuedSequenceNumber}")
      } else {
        Future { movedEvent }
      }
    } else {
      Future { event }
    }
  }

  private def receive(requestSeqNo: SequenceNumber, batchSize: Int, receiveTimeoutHandler: Option[(NameAndPartition, SequenceNumber, Int) => Unit]): Iterator[EventData] = {
    val retryCount = ehConf.receiveRetryTimes.getOrElse(DefaultReceiveRetryTimes)
    var retried = 0
    var result: Option[Iterator[EventData]] = None
    while(retried < retryCount && result.isEmpty) {
      Try {
        // Retrieve the events. First, we get the first event in the batch.
        // Then, if the succeeds, we collect the rest of the data.
        val first = Await.result(checkCursor(requestSeqNo), ehConf.receiverTimeout.map{timeout => Duration.fromNanos(timeout.toNanos)}.getOrElse(DefaultReceiveTimeout))
        val theRest = for {i <- 1 until batchSize} yield
          Await.result(receiveOne(s"receive; $nAndP; seqNo: ${requestSeqNo + i}"),
            ehConf.receiverTimeout.map{timeout => Duration.fromNanos(timeout.toNanos)}.getOrElse(DefaultReceiveTimeout))
        // Combine and sort the data.
        val combined = first ++ theRest.flatten
        val sorted = combined.toSeq
          .sortWith((e1, e2) =>
            e1.getSystemProperties.getSequenceNumber < e2.getSystemProperties.getSequenceNumber)
          .iterator
        val newPrefetchCount =
          if (batchSize < PrefetchCountMinimum) PrefetchCountMinimum else batchSize * 2
        receiver.onComplete {
          case Success(r) => r.setPrefetchCount(newPrefetchCount)
          case _ =>
        }
        val (result, validate) = sorted.duplicate
        assert(validate.size == batchSize)
        result
      } match {
        case Success(iterator) =>
          result = Some(iterator)
        case Failure(exception) =>
          logWarning("Receive timeout", exception)
      }
      retried += 1
    }
    result.getOrElse({
      logWarning(
        s"Abandon the partition: " +
          s"${ConnectionStringBuilder(ehConf.connectionString).getEndpoint.getHost}-${nAndP.ehName}:${nAndP.partitionId}, " +
          s"requestSeqNo: $requestSeqNo, batchSize: $batchSize"
      )
      logInfo(s"TimeoutHandler: $receiveTimeoutHandler")
      receiveTimeoutHandler.foreach(handler => {
        logInfo(s"Execute handler $handler")
        handler.apply(nAndP, requestSeqNo, batchSize)
      })
      Seq.empty[EventData].iterator
    })
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
                                          receiveTimeoutHandler: Option[(NameAndPartition, SequenceNumber, Int) => Unit] = None): Iterator[EventData] = {
    logInfo(s"EventHubsCachedReceiver look up. For $nAndP, ${ehConf.consumerGroup}")
    var receiver: CachedEventHubsReceiver = null
    receivers.synchronized {
      receiver = receivers.getOrElseUpdate(key(ehConf, nAndP), {
        CachedEventHubsReceiver(ehConf, nAndP, requestSeqNo)
      })
    }
    receiver.receive(requestSeqNo, batchSize, receiveTimeoutHandler)
  }

  def apply(ehConf: EventHubsConf,
            nAndP: NameAndPartition,
            startSeqNo: SequenceNumber): CachedEventHubsReceiver = {
    new CachedEventHubsReceiver(
      ehConf,
      nAndP,
      startSeqNo)
  }
}

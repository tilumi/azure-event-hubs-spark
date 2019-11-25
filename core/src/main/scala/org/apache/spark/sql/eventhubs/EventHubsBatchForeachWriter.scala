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

package org.apache.spark.sql.eventhubs

import com.microsoft.azure.eventhubs.{EventData, EventDataBatch, EventHubClient, EventHubException}
import org.apache.spark.eventhubs.{EventHubsConf, _}
import org.apache.spark.eventhubs.client.ClientConnectionPool
import org.apache.spark.eventhubs.utils.RetryUtils._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ForeachWriter, Row}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * A [[ForeachWriter]] to consume data generated by a StreamingQuery.
  * This [[ForeachWriter]] is used to send the generated data to
  * the Event Hub instance specified in the user-provided [[EventHubsConf]].
  * Each partition will use a new deserialized instance, so you usually
  * should do all the initialization (e.g. opening a connection or
  * initiating a transaction) in the open method.
  *
  * This also uses asynchronous send calls which are retried on failure.
  * The retries happen with exponential backoff.
  *
  * @param ehConf the [[EventHubsConf]] containing the connection string
  *               for the Event Hub which will receive the sent events
  */
case class EventHubsBatchForeachWriter(ehConf: EventHubsConf,
                                       eventProperties: Map[String, AnyRef]) extends ForeachWriter[Array[Byte]] with Logging {
  var client: EventHubClient = _
  var eventDataBatch: EventDataBatch = _
  var totalMessageSizeInBytes = 0
  var totalMessageCount = 0
  var writerOpenTime = 0L
  var messageSizeInCurrentBatchInBytes = 0
  var totalBatches = 0
  var totalRetryTimes = 0

  def open(partitionId: Long, version: Long): Boolean = {
    writerOpenTime = System.nanoTime()
    client = ClientConnectionPool.borrowClient(ehConf)
    eventDataBatch = client.createBatch()
    ehConf.senderListener().foreach(_.onWriterOpen(partitionId, version))
    true
  }

  def process(data: Array[Byte]): Unit = {
    val event = EventData.create(data)
    eventProperties.foreach{
      case (key, value) =>
      event.getProperties.put(key,value)
    }
    if (eventDataBatch.tryAdd(event)) {
      messageSizeInCurrentBatchInBytes += event.getBytes.length
    } else {
      sendBatch(eventDataBatch, messageSizeInCurrentBatchInBytes)
      totalMessageCount += eventDataBatch.getSize
      totalMessageSizeInBytes += messageSizeInCurrentBatchInBytes
      totalBatches += 1

      eventDataBatch = client.createBatch()
      messageSizeInCurrentBatchInBytes = 0
      if (eventDataBatch.tryAdd(event)) {
        messageSizeInCurrentBatchInBytes += event.getBytes.length
      } else {
        throw new EventHubException(false, "Even single event is too big to fit into a event batch")
      }
    }
  }

  def close(errorOrNull: Throwable): Unit = {
    errorOrNull match {
      case t: Throwable =>
        ehConf.senderListener().foreach(_.onBatchSendFail(t))
        throw t
      case _ =>
        if (eventDataBatch != null && eventDataBatch.getSize > 0) {
          sendBatch(eventDataBatch, messageSizeInCurrentBatchInBytes)
          totalMessageCount += eventDataBatch.getSize
          totalMessageSizeInBytes += messageSizeInCurrentBatchInBytes
          totalBatches += 1
        }
        ehConf.senderListener().foreach(_.onWriterClose(
          totalMessageCount,
          totalMessageSizeInBytes,
          System.nanoTime() - writerOpenTime,
          Some(totalRetryTimes),
          Some(totalBatches))
        )
        ClientConnectionPool.returnClient(ehConf, client)
    }
  }

  private def sendBatch(currentEventDataBatch: EventDataBatch, messageSizeInCurrentBatchInBytes: Int): Unit = {
    val start = System.nanoTime()
    if(currentEventDataBatch != null && currentEventDataBatch.getSize > 0) {
      Await.result(retryJava(
        client.send(currentEventDataBatch)
      , "EventHubsBatchForeachWriter",
        ehConf.operationRetryTimes.getOrElse(RetryCount),
        ehConf.operationRetryExponentialDelayMs.getOrElse(10)).andThen({
        case Success((_, retryTimes)) =>
          val sendElapsedTimeInNanos = System.nanoTime() - start
          logInfo(s"Send batch to EventHub success! sent ${currentEventDataBatch.getSize} messages, total messages size $messageSizeInCurrentBatchInBytes bytes, elapsed time: ${sendElapsedTimeInNanos / 1000000} milliseconds, retried $retryTimes times, throughput: ${messageSizeInCurrentBatchInBytes / (sendElapsedTimeInNanos / 1000000)} bytes / millisecond")
          ehConf
            .senderListener()
            .foreach(
              _.onBatchSendSuccess(
                currentEventDataBatch.getSize,
                messageSizeInCurrentBatchInBytes,
                sendElapsedTimeInNanos,
                retryTimes
              ))
          totalRetryTimes += retryTimes
        case Failure(exception) =>
          logError(s"Write data to EventHub  '${ehConf.name}' failed!", exception)
          ehConf.senderListener().foreach(_.onBatchSendFail(exception))
          throw exception
      }), Duration.Inf)
    }
  }
}

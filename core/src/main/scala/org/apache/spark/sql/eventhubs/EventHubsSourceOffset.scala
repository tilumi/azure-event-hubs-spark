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

import java.net.URI

import org.apache.spark.eventhubs.{ NameAndPartition, PartitionId, SequenceNumber }
import org.apache.spark.sql.execution.streaming.{ Offset, SerializedOffset }

case class SerializedEventHubsSourceOffset(endpoint: String,
                                           consumerGroup: String,
                                           partitionToSeqNos: Map[String, Map[String, BigInt]])

/**
 * An [[Offset]] for the [[EventHubsSource]]. This tracks all partitions and their offsets.
 */
case class EventHubsSourceOffset(endpoint: URI,
                                 consumerGroup: String,
                                 partitionToSeqNos: Map[NameAndPartition, SequenceNumber])
    extends Offset {

  override val json: String = JsonUtils.serializeOffset(this)
}

/**
 * A companion object of the [[EventHubsSourceOffset]].
 */
object EventHubsSourceOffset {

  def getPartitionSeqNos(offset: Offset): Map[NameAndPartition, SequenceNumber] = {
    offset match {
      case o: EventHubsSourceOffset => o.partitionToSeqNos
      case so: SerializedOffset     => EventHubsSourceOffset(so).partitionToSeqNos
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to EventHubsSourceOffset")
    }
  }

  /**
   * Creates an [[EventHubsSourceOffset]] from a variable sequence of
   * (Event Hub name, partitionId, sequence number) tuples.
   */
  def apply(endpoint: URI,
            consumerGroup: String,
            offsetTuples: (String, Int, SequenceNumber)*): EventHubsSourceOffset = {
    EventHubsSourceOffset(endpoint, consumerGroup, offsetTuples.map {
      case (n, p, s) => (new NameAndPartition(n, p), s)
    }.toMap)
  }

  /**
   * Creates an [[EventHubsSourceOffset]] from a JSON [[SerializedOffset]].
   */
  def apply(offset: SerializedOffset): EventHubsSourceOffset = {
    JsonUtils.deserializeOffset(offset)
  }
}

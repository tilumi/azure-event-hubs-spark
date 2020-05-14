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

import org.apache.spark.eventhubs.{ NameAndPartition, _ }
import org.apache.spark.sql.execution.streaming.SerializedOffset
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Utilities for converting Event Hubs related objects to and from json.
 */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  def main(args: Array[String]): Unit = {
    println(Serialization.read[SerializedEventHubsSourceOffset](
      """{"endpoint":"sb://o365ipdippe-dns.servicebus.windows.net/","consumerGroup":"multi-eventhubs-test","partitionToSeqNos":{"auditenrich":{"0":117975,"5":117953,"10":117761,"56":118234,"42":117989,"24":117809,"37":117938,"25":118053,"52":118098,"14":117907,"20":118109,"46":118072,"57":118023,"29":117922,"61":118022,"1":117800,"6":118017,"60":118026,"28":117842,"38":118105,"21":117924,"33":117998,"9":118075,"53":117940,"13":118059,"41":118044,"2":117867,"32":118054,"34":117987,"45":118021,"17":118097,"22":117788,"44":117891,"59":118138,"27":118036,"12":117830,"54":118103,"49":118023,"7":118062,"39":118173,"3":117907,"35":118055,"48":117831,"63":117913,"18":117920,"50":118189,"16":117906,"31":117961,"11":117904,"43":118166,"40":118181,"26":117946,"55":117738,"23":117908,"8":118047,"58":117999,"36":118003,"30":118012,"51":118046,"19":117965,"4":118073,"47":118050,"15":117970,"62":117816}}}"""))
  }

  /**
   * Read NameAndPartitions from json string
   */
  def partitions(str: String): Array[NameAndPartition] = {
    try {
      Serialization
        .read[Map[String, Seq[PartitionId]]](str)
        .flatMap {
          case (name, parts) =>
            parts.map { part =>
              new NameAndPartition(name, part)
            }
        }
        .toArray
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"ehNameA":[0,1],"ehNameB":[0,1]}, got $str""")
    }
  }

  /**
   * Write NameAndPartitions as json string
   */
  def partitions(partitions: Iterable[NameAndPartition]): String =
    Serialization.write(partitions.groupBy(_.ehName))

  def serializeOffset(offset: EventHubsSourceOffset): String = {
    Serialization.write(
      SerializedEventHubsSourceOffset(
        offset.endpoint.toString,
        offset.consumerGroup,
        JsonUtils
          .partitionSeqNos(offset.partitionToSeqNos)
          .mapValues(value =>
            value.map {
              case (partitionId, sequenceNumber) =>
                partitionId.toString -> BigInt(sequenceNumber)
          })
      ))
  }

  def deserializeOffset(offset: SerializedOffset): EventHubsSourceOffset = {
    try {
      val serializedEventHubsSourceOffset =
        Serialization.read[SerializedEventHubsSourceOffset](offset.json)
      EventHubsSourceOffset.apply(
        new URI(serializedEventHubsSourceOffset.endpoint),
        serializedEventHubsSourceOffset.consumerGroup,
        serializedEventHubsSourceOffset.partitionToSeqNos.flatMap {
          case (name, partSeqNos) =>
            partSeqNos.map {
              case (part, seqNo) =>
                NameAndPartition(name, part.toInt) -> seqNo.toLong
            }
        }
      )
    } catch {
      case NonFatal(exception: Exception) =>
        throw new IllegalArgumentException(
          s"failed to parse ${offset.json}\n" +
            s"""Expected e.g. {"endpoint": "...", "consumerGroup": "...", "partitionToSeqNos": {"ehName":{"0":23,"1":-1},"ehNameB":{"0":-2}}""",
          exception
        )
    }
  }

  /**
   * Write per-NameAndPartition seqNos as json string
   */
  def partitionSeqNos(partitionSeqNosValue: Map[NameAndPartition, SequenceNumber])
    : Map[String, Map[PartitionId, SequenceNumber]] = {
    val result = new mutable.HashMap[String, mutable.HashMap[PartitionId, SequenceNumber]]()
    implicit val ordering = new Ordering[NameAndPartition] {
      override def compare(x: NameAndPartition, y: NameAndPartition): Int = {
        Ordering
          .Tuple2[String, PartitionId]
          .compare((x.ehName, x.partitionId), (y.ehName, y.partitionId))
      }
    }
    val partitions = partitionSeqNosValue.keySet.toSeq.sorted // sort for more determinism
    partitions.foreach { nAndP =>
      val seqNo = partitionSeqNosValue(nAndP)
      val parts = result.getOrElse(nAndP.ehName, new mutable.HashMap[PartitionId, SequenceNumber])
      parts += nAndP.partitionId -> seqNo
      result += nAndP.ehName -> parts
    }
    result.mapValues(_.toMap).toMap
  }
}

package org.apache.spark.eventhubs.utils

import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.client.EventHubsClient
import org.apache.spark.sql.DataFrame

class EventHubQueryTool(eventHubsConf: EventHubsConf) {

  def query(sql: String): DataFrame = {
    new EventHubsClient(eventHubsConf)
  }

}

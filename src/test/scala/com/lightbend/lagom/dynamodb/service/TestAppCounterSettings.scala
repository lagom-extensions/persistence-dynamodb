package com.lightbend.lagom.dynamodb.service

import com.typesafe.config.ConfigFactory

final class TestAppCounterSettings {

  private val rootConfig = ConfigFactory.load
  private val appConfig = rootConfig.getConfig("app")

  val TestCounterStateTableName: String =
    appConfig.getString("persistence.dynamodb.table.counter-state.table-name")
  val TestCounterStateReadCapacityUnits: Long =
    appConfig.getInt("persistence.dynamodb.table.counter-state.read-capacity-units")
  val TestCounterStateWriteCapacityUnits: Long =
    appConfig.getInt("persistence.dynamodb.table.counter-state.write-capacity-units")
}

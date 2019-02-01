package com.lightbend.lagom.internal.persistence.dynamodb

import javax.inject.Inject
import akka.actor.ActorSystem

/**
  * Internal API
  */
private[lagom] class DynamoDBReadSideSettings @Inject() (system: ActorSystem) {
  private val dynamoDBConfig = system.settings.config.getConfig("lagom.persistence.read-side.dynamodb")

  val autoCreateTables: Boolean = dynamoDBConfig.getBoolean("tables-autocreate")
  val offsetStoreTableName: String = dynamoDBConfig.getString("offset-store.table-name")
  val offsetStoreReadCapacityUnits: Long = dynamoDBConfig.getLong("offset-store.read-capacity-units")
  val offsetStoreWriteCapacityUnits: Long = dynamoDBConfig.getLong("offset-store.write-capacity-units")
}

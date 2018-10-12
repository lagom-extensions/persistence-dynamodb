package com.lightbend.lagom.internal.persistence.dynamodb

import akka.actor.ActorSystem
import javax.inject.Inject

/**
  * Internal API
  */
private[lagom] class DynamoDBWriteSideSettings @Inject()(system: ActorSystem) {
  val autoCreateTables: Boolean = system.settings.config.getBoolean("lagom.persistence.dynamodb.tables-autocreate")
}

package com.lightbend.lagom.internal.persistence.dynamodb

import com.gu.scanamo.Table

trait DynamoDBSchema {
  val dynamoDBReadSideSettings: DynamoDBReadSideSettings
  val offsetStoreTable = Table[OffsetStoreEntity](dynamoDBReadSideSettings.offsetStoreTableName)
}

case class OffsetStoreEntity(eventProcessorId: String,
                             tag: String,
                             sequenceOffset: Option[Long])
package com.lightbend.lagom.dynamodb

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.internal.persistence.dynamodb.{DynamoDBOffsetStore, DynamoDBReadSideSettings}

import scala.concurrent.ExecutionContext

/**
  * Internal API
  */
private[lagom] final class ScaladslDynamoDBOffsetStore(system: ActorSystem,
                                                       dynamoClient: DynamoClient,
                                                       dynamoDBReadSideSettings: DynamoDBReadSideSettings,
                                                       config: ReadSideConfig)(implicit ec: ExecutionContext, materializer: Materializer)
  extends DynamoDBOffsetStore(system, dynamoClient, dynamoDBReadSideSettings, config)

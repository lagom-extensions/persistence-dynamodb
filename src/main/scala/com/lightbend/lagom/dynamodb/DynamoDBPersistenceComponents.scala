package com.lightbend.lagom.dynamodb

import akka.actor.PoisonPill
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.lightbend.lagom.internal.persistence.dynamodb.stream.{DynamoDBAkkaPersistenceEventsProviderActor, DynamoDBSerializerRegistry, StartDynamoDBStreaming}
import com.lightbend.lagom.internal.persistence.dynamodb.{DynamoDBOffsetStore, DynamoDBPersistenceConfig, DynamoDBReadSideSettings, DynamoDBWriteSideSettings}
import com.lightbend.lagom.scaladsl.persistence._
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.lightbend.lagom.spi.persistence.OffsetStore

import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Persistence DynamoDB components (for compile-time injection).
  */
trait DynamoDBPersistenceComponents extends PersistenceComponents with ReadSideDynamoDBPersistenceComponents with WriteSideDynamoDBPersistenceComponents {
  override lazy val readSideStreamEnabled = true
  override def optionalJsonSerializerRegistry: Option[JsonSerializerRegistry] = Some(DynamoDBSerializerRegistry)
}

/**
  * Write-side persistence DynamoDB components (for compile-time injection).
  */
trait WriteSideDynamoDBPersistenceComponents extends WriteSidePersistenceComponents {
  def executionContext: ExecutionContext
  def materializer: Materializer
  def dynamoClient: DynamoClient
  lazy val readSideStreamEnabled = false
  lazy val journalTable = persistentEntityRegistry.journalTable
  lazy val dynamoDBWriteSideSettings: DynamoDBWriteSideSettings = new DynamoDBWriteSideSettings(actorSystem)
  override lazy val persistentEntityRegistry = new DynamoDBPersistentEntityRegistry(
    dynamoClient,
    dynamoDBWriteSideSettings,
    readSideStreamEnabled
  )(actorSystem, executionContext)
}

/**
  * Read-side persistence DynamoDB components (for compile-time injection).
  */
trait ReadSideDynamoDBPersistenceComponents extends ReadSidePersistenceComponents {
  def journalTable: Future[TableDescription]
  lazy val readSideStreamEnabled = true
  lazy val settings = DynamoSettings(actorSystem)
  lazy val dynamoClient = DynamoClient(settings)(actorSystem, materializer)
  lazy val dynamoDBReadSideSettings: DynamoDBReadSideSettings = new DynamoDBReadSideSettings(actorSystem)

  private[lagom] lazy val dynamoDBOffsetStore: DynamoDBOffsetStore = new ScaladslDynamoDBOffsetStore(actorSystem, dynamoClient, dynamoDBReadSideSettings, readSideConfig)
  lazy val offsetStore: OffsetStore = dynamoDBOffsetStore
  lazy val dynamoDBReadSide: DynamoDBReadSide = new DynamoDBReadSideImpl(actorSystem, dynamoClient, dynamoDBOffsetStore)

  private val globalPrepareTimeout = DynamoDBPersistenceConfig.globalPrepareTimeout(actorSystem)
  private val journalTableDescription: TableDescription = Await.result(journalTable, globalPrepareTimeout)

  private val role = configuration.underlying.getString("lagom.persistence.run-entities-on-role")
  actorSystem.actorOf(
    ClusterSingletonManager.props(
      singletonProps = DynamoDBAkkaPersistenceEventsProviderActor.props(journalTableDescription),
      terminationMessage = PoisonPill,
      settings = ClusterSingletonManagerSettings(actorSystem).withRole(role)
    ),
    name = DynamoDBAkkaPersistenceEventsProviderActor.name
  )

  if (!readSideStreamEnabled) {
    throw new IllegalStateException(
      "Try to start with ReadSidePersistenceComponents, but no read side processors exists, or traits mixed order makes read side stream key disabled"
    )
  } else {
    import scala.concurrent.ExecutionContext.Implicits.global
    actorSystem.scheduler.scheduleOnce(
      globalPrepareTimeout,
      new Runnable {
        override def run(): Unit =
          DynamoDBAkkaPersistenceEventsProviderActor.dynamoStreamingActorProxy(actorSystem) ! StartDynamoDBStreaming
      }
    )
  }
}

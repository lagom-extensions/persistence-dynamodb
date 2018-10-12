package com.lightbend.lagom.dynamodb

import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.lightbend.lagom.internal.persistence.dynamodb.stream.DynamoDBAkkaPersistenceEventsProvider
import com.lightbend.lagom.internal.persistence.dynamodb.{DynamoDBOffsetStore, DynamoDBPersistenceConfig, DynamoDBReadSideSettings, DynamoDBWriteSideSettings}
import com.lightbend.lagom.scaladsl.persistence._
import com.lightbend.lagom.spi.persistence.OffsetStore

import scala.concurrent.{Await, ExecutionContext, Future, Promise}

/**
  * Persistence DynamoDB components (for compile-time injection).
  */
trait DynamoDBPersistenceComponents extends PersistenceComponents
  with ReadSideDynamoDBPersistenceComponents
  with WriteSideDynamoDBPersistenceComponents {
  override lazy val readSideStreamEnabled = true
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
  override lazy val persistentEntityRegistry = new DynamoDBPersistentEntityRegistry(dynamoClient, dynamoDBWriteSideSettings, readSideStreamEnabled)(actorSystem, executionContext, materializer)
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

  // can this be smarter, something like Set[AggregateEventTag[Event forSome {type Event <: AggregateEvent[Event]}]]
  lazy val readSideTags = Set[String]()

  private[lagom] lazy val dynamoDBOffsetStore: DynamoDBOffsetStore =
    new ScaladslDynamoDBOffsetStore(actorSystem, dynamoClient, dynamoDBReadSideSettings, readSideConfig)(executionContext, materializer)
  lazy val offsetStore: OffsetStore = dynamoDBOffsetStore

  lazy val dynamoDBReadSide: DynamoDBReadSide = new DynamoDBReadSideImpl(actorSystem, dynamoClient, dynamoDBOffsetStore)

  import scala.concurrent.duration._

  val journalTableDescription: TableDescription = Await.result(journalTable, 1.minute)
  DynamoDBAkkaPersistenceEventsProvider.init(journalTableDescription)(actorSystem)
  actorSystem.registerOnTermination(() => DynamoDBAkkaPersistenceEventsProvider.shutdown)

  // todo make this better
  if (!readSideStreamEnabled || readSideTags.isEmpty) {
    throw new IllegalStateException("Try to start with ReadSidePersistenceComponents, but no read side processors exists, or traits mixed order makes read side stream key disabled")
  } else {
    import scala.concurrent.ExecutionContext.Implicits.global
    val maximumInitializationTime = System.currentTimeMillis() + DynamoDBPersistenceConfig.globalPrepareTimeout(actorSystem)
    val delay: FiniteDuration = 1.second
    def isReady = readSideTags.size == DynamoDBAkkaPersistenceEventsProvider.registeredConsumersCount
    def awaitReady(): Future[Boolean] = {
      val eventualReady: Future[Boolean] = if (isReady) Future(true) else Future.failed(new ExceptionInInitializerError())
      eventualReady.recoverWith {
        case _ if System.currentTimeMillis() < maximumInitializationTime =>
          val timeout = Promise[Boolean]()
          actorSystem.scheduler.scheduleOnce(delay)(timeout.completeWith(awaitReady()))
          timeout.future
        case _ => Future.failed(new ExceptionInInitializerError())
      }
    }
    awaitReady().onComplete {
      case util.Success(_) =>
        DynamoDBAkkaPersistenceEventsProvider.startStreaming(actorSystem)
      case util.Failure(_) =>
        actorSystem.terminate()
        throw new ExceptionInInitializerError("Failed to start DynamoDB Streaming")
    }
  }
}

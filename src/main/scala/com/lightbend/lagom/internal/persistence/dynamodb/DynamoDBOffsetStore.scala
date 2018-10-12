package com.lightbend.lagom.internal.persistence.dynamodb

import akka.Done
import akka.actor.ActorSystem
import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import akka.stream.Materializer
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits._
import akka.util.Timeout
import com.amazonaws.services.dynamodbv2.model._
import com.gu.scanamo._
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.syntax._
import com.lightbend.lagom.internal.persistence.ReadSideConfig
import com.lightbend.lagom.internal.persistence.cluster.ClusterStartupTask
import com.lightbend.lagom.spi.persistence.{OffsetDao, OffsetStore}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Internal API
  */
private[lagom] abstract class DynamoDBOffsetStore(system: ActorSystem,
                                                  dynamoClient: DynamoClient,
                                                  override val dynamoDBReadSideSettings: DynamoDBReadSideSettings,
                                                  config: ReadSideConfig
                                                 )(implicit materializer: Materializer) extends OffsetStore with DynamoDBSchema {

  import system.dispatcher

  override def prepare(eventProcessorId: String, tag: String): Future[DynamoDBOffsetDao] = {
    implicit val timeout = Timeout(config.globalPrepareTimeout)
    doPrepare(eventProcessorId, tag).map { offset => new DynamoDBOffsetDao(dynamoClient, eventProcessorId, tag, offset, dynamoDBReadSideSettings) }
  }

  val startupTask = if (dynamoDBReadSideSettings.autoCreateTables) {
    Some(
      ClusterStartupTask(
        system,
        "dynamoDBOffsetStorePrepare",
        () => createTable().recover { case _: ResourceInUseException => Done },
        config.globalPrepareTimeout,
        config.role,
        config.minBackoff,
        config.maxBackoff,
        config.randomBackoffFactor
      )
    )
  } else None

  private def createTable(): Future[Done] = dynamoClient.single(
    new CreateTableRequest()
      .withTableName(dynamoDBReadSideSettings.offsetStoreTableName)
      .withKeySchema(
        new KeySchemaElement("eventProcessorId", KeyType.HASH),
        new KeySchemaElement("tag", KeyType.RANGE))
      .withAttributeDefinitions(
        new AttributeDefinition("eventProcessorId", ScalarAttributeType.S),
        new AttributeDefinition("tag", ScalarAttributeType.S))
      .withProvisionedThroughput(
        new ProvisionedThroughput().withReadCapacityUnits(dynamoDBReadSideSettings.offsetStoreReadCapacityUnits).withWriteCapacityUnits(dynamoDBReadSideSettings.offsetStoreWriteCapacityUnits)))
    .map(_ => Done)

  protected def doPrepare(eventProcessorId: String, tag: String): Future[Offset] = {
    implicit val timeout = Timeout(config.globalPrepareTimeout)
    for {
      _ <- startupTask.fold(Future.successful[Done](Done))(task => task.askExecute)
      offset <- readOffset(eventProcessorId, tag)
    } yield offset
  }

  private def readOffset(eventProcessorId: String, tag: String): Future[Offset] = {
    val ops = for {
      e <- offsetStoreTable.query('eventProcessorId -> eventProcessorId and ('tag -> tag))
    } yield e
    val futureList = for {
      list <- ScanamoAlpakka.exec(dynamoClient)(ops)
    } yield list
    futureList.map {
      case eitherOffset :: _ =>
        eitherOffset.fold(
          _ => NoOffset,
          offsetStore => offsetStore.sequenceOffset match {
            case Some(sequenceOffset) => Sequence(sequenceOffset)
            case None => NoOffset
          }
        )
      case Nil => NoOffset
    }
  }
}

/**
  * Internal API
  */
private[lagom] final class DynamoDBOffsetDao(dynamoClient: DynamoClient,
                                             eventProcessorId: String,
                                             tag: String,
                                             override val loadedOffset: Offset,
                                             override val dynamoDBReadSideSettings: DynamoDBReadSideSettings)(implicit ec: ExecutionContext) extends OffsetDao with DynamoDBSchema {
  override def saveOffset(offset: Offset): Future[Done] = {
    val ops = for {
      e <- offsetStoreTable.put(toOffsetStoreEntity(offset))
    } yield e
    ScanamoAlpakka.exec(dynamoClient)(ops)
      .flatMap((optEither: Option[Either[DynamoReadError, OffsetStoreEntity]]) => {
        optEither.map(_ => Future.successful(Done))
          .getOrElse(Future.failed[Done](new OffsetStoreException()))
      })
  }

  def bindSaveOffset(offset: Offset): ScanamoOps[_] = offsetStoreTable.put(toOffsetStoreEntity(offset))

  private def toOffsetStoreEntity(offset: Offset): OffsetStoreEntity = {
    offset match {
      case NoOffset => OffsetStoreEntity(eventProcessorId, tag, None)
      case seq: Sequence => OffsetStoreEntity(eventProcessorId, tag, Some(seq.value))
      case _: TimeBasedUUID => throw new TimeBaseOffsetNotSupportedException
    }
  }
}

class OffsetStoreException extends RuntimeException()
class TimeBaseOffsetNotSupportedException extends RuntimeException()
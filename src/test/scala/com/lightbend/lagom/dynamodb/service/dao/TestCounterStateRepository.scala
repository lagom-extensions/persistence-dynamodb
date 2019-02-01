package com.lightbend.lagom.dynamodb.service.dao

import akka.Done
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits.CreateTable
import com.amazonaws.services.dynamodbv2.model._
import com.gu.scanamo.ScanamoAlpakka
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.ops.ScanamoOps
import com.lightbend.lagom.dynamodb.DynamoDBReadSide
import com.lightbend.lagom.dynamodb.service._
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

private[lagom] class TestCounterStateRepository(
    val appCounterSettings: TestAppCounterSettings,
    dynamoClient: DynamoClient
)(
    implicit ec: ExecutionContext
) extends TestCounterDBSchema {

  def getAllCounters(): Future[TestCounterStatistic] = {
    val ops = for {
      allLines <- counterStateTable.scan()
    } yield allLines
    ScanamoAlpakka
      .exec(dynamoClient)(ops)
      .map((listEither: Seq[Either[DynamoReadError, TestCounterState]]) => {
        // todo cats Alternative.separate
        val (_, counterSafeEither) = listEither.partition(_.isLeft)
        TestCounterStatistic(counterSafeEither.map(_.right.get))
      })
  }

  def insertCounterState(
      counterEntityId: String,
      counterAmount: BigDecimal
  ): Future[ScanamoOps[_]] =
    Future(counterStateTable.put(TestCounterState(counterEntityId, counterAmount)))
}

private[lagom] class CounterEventProcessor(
    settings: TestAppCounterSettings,
    dynamoClient: DynamoClient,
    readSide: DynamoDBReadSide,
    counterStateRepository: TestCounterStateRepository
)(implicit executionContext: ExecutionContext)
    extends ReadSideProcessor[TestCounterEvent] {

  override def aggregateTags = TestCounterEvent.Tag.allTags

  override def buildHandler = {
    readSide
      .builder[TestCounterEvent]("CounterEntityOffset")
      .setGlobalPrepare(() => createTable())
      .setEventHandler[TestCounterUpdatedEvent](
        e =>
          counterStateRepository
            .insertCounterState(e.entityId, e.event.nextState)
      )
      .build
  }

  private def createTable(): Future[Done] =
    dynamoClient
      .single(
        new CreateTableRequest()
          .withTableName(settings.TestCounterStateTableName)
          .withKeySchema(new KeySchemaElement("counter", KeyType.HASH))
          .withAttributeDefinitions(
            new AttributeDefinition("counter", ScalarAttributeType.S)
          )
          .withProvisionedThroughput(
            new ProvisionedThroughput()
              .withReadCapacityUnits(settings.TestCounterStateReadCapacityUnits)
              .withWriteCapacityUnits(settings.TestCounterStateWriteCapacityUnits)
          )
      )
      .map(_ => Done)
      .recover { case _: ResourceInUseException => Done }
}

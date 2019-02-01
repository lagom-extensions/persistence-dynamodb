package com.lightbend.lagom.dynamodb

import akka.Done
import akka.actor.{ActorSystem, Terminated}
import akka.persistence.dynamodb.{journal, snapshot}
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits.{CreateTable, DescribeTable}
import com.amazonaws.services.dynamodbv2.model._
import com.lightbend.lagom.internal.persistence.dynamodb.DynamoDBPersistenceConfig._
import com.lightbend.lagom.internal.persistence.dynamodb.{DynamoDBReadJournal, DynamoDBWriteSideSettings}
import com.lightbend.lagom.internal.scaladsl.persistence.AbstractPersistentEntityRegistry

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Internal API
  */
private[lagom] final class DynamoDBPersistentEntityRegistry(dynamoClient: DynamoClient,
                                                            dynamoDBWriteSideSettings: DynamoDBWriteSideSettings,
                                                            readSideStreamEnabled: Boolean)
                                                           (implicit system: ActorSystem, executionContext: ExecutionContext)
  extends AbstractPersistentEntityRegistry(system) {
  lazy val journalTable: Future[TableDescription] = {
    if (isTablesAutoCreate) createJournalTable(journalTableName)
    else getJournalTableDescription(journalTableName)
  }

  validateDynamoPersistenceConfig(system)
  journalTable.onComplete {
    case Success(_) => if (isTablesAutoCreate) createSnapshotStoreTable(snapshotTableName(system)).recoverWith(logAndTerminate)
    case Failure(_) => logAndTerminate(_)
  }

  override protected val queryPluginId = Some(DynamoDBReadJournal.Identifier)

  private def createJournalTable(journalTableName: String): Future[TableDescription] = dynamoClient.single(
    new CreateTableRequest()
      .withTableName(journalTableName)
      .withKeySchema(
        new KeySchemaElement(journal.Key, KeyType.HASH),
        new KeySchemaElement(journal.Sort, KeyType.RANGE))
      .withAttributeDefinitions(
        new AttributeDefinition(journal.Key, ScalarAttributeType.S),
        new AttributeDefinition(journal.Sort, ScalarAttributeType.N))
      .withProvisionedThroughput(
        new ProvisionedThroughput().withReadCapacityUnits(journalReadCapacityUnits).withWriteCapacityUnits(journalWriteCapacityUnits))
      .withStreamSpecification(new StreamSpecification().withStreamEnabled(readSideStreamEnabled).withStreamViewType(StreamViewType.NEW_IMAGE)))
    .map(createTableResponse => createTableResponse.getTableDescription)
    .recoverWith {
      case _: ResourceInUseException => getJournalTableDescription(journalTableName)
    }

  private def getJournalTableDescription(journalTableName: String): Future[TableDescription] =
    dynamoClient.single(new DescribeTableRequest().withTableName(journalTableName))
      .map((descTableResp: DescribeTableResult) => descTableResp.getTable)

  private def createSnapshotStoreTable(snapshotTableName: String): Future[Done] = dynamoClient.single(
    new CreateTableRequest()
      .withTableName(snapshotTableName)
      .withKeySchema(
        new KeySchemaElement(snapshot.Key, KeyType.HASH),
        new KeySchemaElement(snapshot.SequenceNr, KeyType.RANGE))
      .withAttributeDefinitions(
        new AttributeDefinition(snapshot.Key, ScalarAttributeType.S),
        new AttributeDefinition(snapshot.SequenceNr, ScalarAttributeType.N),
        new AttributeDefinition(snapshot.Timestamp, ScalarAttributeType.N))
      .withLocalSecondaryIndexes(
        new LocalSecondaryIndex().withIndexName(snapshot.TimestampIndex)
          .withKeySchema(
            new KeySchemaElement().withAttributeName(snapshot.Key).withKeyType(KeyType.HASH),
            new KeySchemaElement().withAttributeName(snapshot.Timestamp).withKeyType(KeyType.RANGE))
          .withProjection(new Projection().withProjectionType(ProjectionType.ALL)))
      .withProvisionedThroughput(
        new ProvisionedThroughput().withReadCapacityUnits(snapshotReadCapacityUnits).withWriteCapacityUnits(snapshotWriteCapacityUnits)))
    .map(_ => Done)
    .recover { case _: ResourceInUseException => Done }

  private def logAndTerminate: PartialFunction[Throwable, Future[Terminated]] = {
    case e: Throwable =>
      system.log.error("Failed with creation DynamoDB persistence tables [{}]", e.getMessage)
      system.terminate()
  }

  private def isTablesAutoCreate: Boolean = dynamoDBWriteSideSettings.autoCreateTables
}
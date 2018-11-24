package com.lightbend.lagom.internal.persistence.dynamodb.stream

import java.util

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.dynamodb.journal
import akka.persistence.journal.Tagged
import akka.persistence.query.{NoOffset, Sequence}
import akka.serialization.Serialization
import akka.util.ByteString
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.lightbend.lagom.internal.persistence.dynamodb.DynamoDBPersistenceConfig
import com.lightbend.lagom.internal.scaladsl.persistence.PersistentEntityActor.EntityIdSeparator
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}

private[stream] class DynamoDBStreamActorEventsRecordProcessor(
                                                                dynamoDBClient: AmazonDynamoDB,
                                                                serializer: Serialization,
                                                                system: ActorSystem,
                                                                eventsProvider: DynamoDBAkkaPersistenceEventsProviderActor
                                                              ) extends IRecordProcessor {

  private val log = LoggerFactory.getLogger(getClass)
  private val handleOnlyMaxInChunks = DynamoDBPersistenceConfig.readSideProcessOnlyMaxByOffset(system)
  private val recordIngestionTimeout = DynamoDBPersistenceConfig.readSideDynamoDBAwaitRecordIngestionTimeout(system)

  override def initialize(shardId: String): Unit = {}

  override def processRecords(dynamoRecords: util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    if (dynamoRecords.nonEmpty) {
      val journalRecords: immutable.Seq[RecordAdapter] = dynamoRecords.toList
        .filter(_.isInstanceOf[RecordAdapter])
        .map(_.asInstanceOf[RecordAdapter])
        .filter(_.getInternalObject.getEventName == "INSERT")
        .filter(_.getInternalObject.getDynamodb.getNewImage.contains(journal.Key))
        .filter(_.getInternalObject.getDynamodb.getNewImage.get(journal.Key).getS.contains(EntityIdSeparator))

      val readyForIngestion: Seq[RecordData] = toReadyForIngestionRecords(journalRecords)
      val filteredHavingConsumers: Seq[ReadyForIngestionRecord] = toReadyForIngestionByConsumerAndOffset(readyForIngestion)
      val filtered: Seq[ReadyForIngestionRecord] = toReadyForIngestionByMaxOffset(filteredHavingConsumers)

      filtered.foreach(item => {
        // with recommended design for read side to react only on last in chunk, this blocking is not a big deal
        Try {
          Await.result(eventsProvider.handleEvent(item), recordIngestionTimeout)
        } match {
          case Success(_) => log.info(s"Success complete read side data ingestion for record ${item.recordData.recordAdapter}")
          case Failure(exception) =>
            log.error(s"Failed complete read side data ingestion for record ${item.recordData.recordAdapter} , Exception ${exception.getMessage}")
            throw exception
        }
      })
      Try(checkpointer.checkpoint(dynamoRecords.reverse.head))
    }
  }

  private def toReadyForIngestionRecords(journalRecords: immutable.Seq[RecordAdapter]): Seq[RecordData] = {
    val (parseableRecords, notParseableRecords) = journalRecords
      .map(record => (record, toTagEventData(record)))
      .partition(_._2.isRight)
    if (notParseableRecords.nonEmpty) log.error(s"Ignore DynamoDB records as parse error, we don't retry as this mostly failed again. Records: $notParseableRecords")
    parseableRecords.map(_._2.right.get)
  }

  private def toReadyForIngestionByConsumerAndOffset(records: Seq[RecordData]): Seq[ReadyForIngestionRecord] = {
    val (consumersForRecord, noConsumerRecords) = records
      .map(r => (r, eventsProvider.getConsumerActorByEventTag(r.tag)))
      .partition(_._2.isDefined)
    if (noConsumerRecords.nonEmpty) log.error(s"Ignore DynamoDB records as no appropriate tag consumers. Records: $noConsumerRecords")

    val (validByOffset, notValidByOffset) = consumersForRecord
      .partition(item => {
        val recordOffset = item._1.offset
        val tagOffsetConsumers = item._2
        tagOffsetConsumers.exists(tagOffset => eventsProvider.matchByOffset(fromOffset = tagOffset.sequenceOffsetOpt.getOrElse(NoOffset), recordOffset = recordOffset))
      })
    if (notValidByOffset.nonEmpty) log.warn(s"Ignore DynamoDB records as late events by already processed offset. Records: $notValidByOffset")

    validByOffset.map(item => ReadyForIngestionRecord(item._1, item._2.get))
  }

  private def toReadyForIngestionByMaxOffset(records: Seq[ReadyForIngestionRecord]): Seq[ReadyForIngestionRecord] = {
    if (!handleOnlyMaxInChunks) records
    else {
      def max(r1: ReadyForIngestionRecord, r2: ReadyForIngestionRecord): ReadyForIngestionRecord = if (r2.recordData.offset.compareTo(r1.recordData.offset) > 0) r2 else r1
      val mMap = new util.LinkedHashMap[String, ReadyForIngestionRecord]()
      for (record <- records) {
        val key = record.recordData.tag
        val curRecord = mMap.get(key, record)
        if (curRecord == null) {
          mMap.put(key, record)
        } else {
          mMap.put(key, max(curRecord, record))
        }
      }
      mMap.values().toList
    }
  }

  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {
    if (reason eq ShutdownReason.TERMINATE) Try(checkpointer.checkpoint())
  }

  private def toTagEventData(recordAdapter: RecordAdapter): Either[Throwable, RecordData] = {
    try {
      val newEvent = recordAdapter.getInternalObject.getDynamodb.getNewImage
      val persistentRepr = serializer.deserialize(ByteString(newEvent.get(journal.Payload).getB).toArray, classOf[PersistentRepr]).get
      val seqNumber = persistentRepr.sequenceNr
      val offset = Sequence(Try(recordAdapter.getSequenceNumber.toLong).getOrElse(0L))
      val tagged = persistentRepr.payload.asInstanceOf[Tagged]
      Right(
        RecordData(
          recordAdapter,
          tagged.tags.head,
          offset = offset,
          persistenceId = persistentRepr.persistenceId,
          sequenceNr = seqNumber,
          serializedTaggedEventBytes = serializer.serialize(tagged).get
        )
      )
    } catch {
      case ex: Throwable => Left(ex)
    }
  }
}

private[stream] case class DynamoDBTryRecordResult(record: RecordAdapter)
private[stream] case class RecordData(recordAdapter: RecordAdapter, tag: String, offset: Sequence, persistenceId: String, sequenceNr: Long, serializedTaggedEventBytes: Array[Byte])
private[stream] case class ReadyForIngestionRecord(recordData: RecordData, consumer: TagByOffsetConsumerActor)

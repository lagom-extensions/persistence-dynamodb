package com.lightbend.lagom.internal.persistence.dynamodb.stream

import java.util

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.dynamodb.journal
import akka.persistence.journal.Tagged
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, Sequence}
import akka.serialization.Serialization
import akka.stream.QueueOfferResult
import akka.util.ByteString
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.lightbend.lagom.internal.persistence.dynamodb.DynamoDBPersistenceConfig
import com.lightbend.lagom.internal.persistence.dynamodb.stream.DynamoDBAkkaPersistenceEventsProvider.OffsetQueuePair
import com.lightbend.lagom.internal.scaladsl.persistence.PersistentEntityActor.EntityIdSeparator
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}

private[stream] class DynamoDBStreamActorEventsRecordProcessor(dynamoDBClient: AmazonDynamoDB, serializer: Serialization, system: ActorSystem) extends IRecordProcessor {

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

      val readyForIngestion: Seq[RecordTagEventEnvelopData] = toReadyForIngestionRecords(journalRecords)
      val filteredHavingConsumers: Seq[ReadyForIngestionRecord] = toReadyForIngestionByConsumerAndOffset(readyForIngestion)
      val filtered: Seq[ReadyForIngestionRecord] = toReadyForIngestionByMaxOffset(filteredHavingConsumers)

      filtered.foreach(
        item => {
          val sourceQueueWithComplete = item.offsetQueuePair._2
          val eventEnvelope = item.recordData.eventEnvelope
          val dynamoDBRecord = item.recordData.recordAdapter
          val tag = item.recordData.tag
          val dynamoRecordOffset = eventEnvelope.offset
          // with recommended design for read side to react only on last in chunk, this blocking is not a big deal
          // https://stackoverflow.com/questions/44202074/akka-stream-source-queue-hangs-when-using-fail-overflow-strategy
          Try {
            Await.result(sourceQueueWithComplete.offer(eventEnvelope), recordIngestionTimeout) match {
              case QueueOfferResult.Dropped => throw new IllegalStateException(s"ignore all next elements in chunk of dynamoDB Stream elements as dropped offered element : $dynamoDBRecord ")
              case QueueOfferResult.QueueClosed => throw new IllegalStateException(s"ignore all next elements in chunk of dynamoDB Stream elements as stream was completed during future was active : $dynamoDBRecord ")
              case QueueOfferResult.Failure(f) => throw new IllegalStateException(s"ignore all next elements in chunk of dynamoDB Stream elements as failed to offer element :$dynamoDBRecord  cause: $f")
              case QueueOfferResult.Enqueued => DynamoDBAkkaPersistenceEventsProvider.updateOffsetForTag(tag, dynamoRecordOffset)
            }
          } match {
            case Success(_) => log.info(s"Success complete read side data ingestion for record $eventEnvelope")
            case Failure(exception) =>
              log.error(s"Failed complete read side data ingestion for record $eventEnvelope , Exception ${exception.getMessage}")
              throw exception
          }
        }
      )
      Try(checkpointer.checkpoint(dynamoRecords.reverse.head))
    }
  }

  private def toReadyForIngestionRecords(journalRecords: immutable.Seq[RecordAdapter]): Seq[RecordTagEventEnvelopData] = {
    val (parseableRecords, notParseableRecords) = journalRecords
      .map(record => (record, toTagEventData(record)))
      .partition(_._2.isDefined)
    if (notParseableRecords.nonEmpty) log.error(s"Ignore DynamoDB records as parse error, we don't retry as this mostly failed again. Records: $notParseableRecords")
    parseableRecords.map(_._2.get)
  }

  private def toReadyForIngestionByConsumerAndOffset(records: Seq[RecordTagEventEnvelopData]): Seq[ReadyForIngestionRecord] = {
    def matchByOffset(fromOffset: Offset, recordOffset: Offset): Boolean = {
      (fromOffset, recordOffset) match {
        case (NoOffset, _) => true
        case (Sequence(from), Sequence(actual)) => from < actual
        case _ => false
      }
    }

    val (recordsWithConsumersOpt, noConsumerRecords) = records
      .map(r => (r, DynamoDBAkkaPersistenceEventsProvider.getOffsetQueuePairByTag(r.tag)))
      .partition(_._2.isDefined)
    if (noConsumerRecords.nonEmpty) log.error(s"Ignore DynamoDB records as no appropriate tag consumers. Records: $noConsumerRecords")

    val recordsWithConsumers = recordsWithConsumersOpt
      .map { case (recordTagEventEnvelopData, offsetQueuePairOpt) => (recordTagEventEnvelopData, offsetQueuePairOpt.get) }

    val (validByOffset, notValidByOffset) = recordsWithConsumers
      .partition(item => matchByOffset(fromOffset = item._2._1, recordOffset = item._1.eventEnvelope.offset))
    if (notValidByOffset.nonEmpty) log.warn(s"Ignore DynamoDB records as late events by already processed offset. Records: $notValidByOffset")

    validByOffset.map(item => ReadyForIngestionRecord(item._1, item._2))
  }

  private def toReadyForIngestionByMaxOffset(records: Seq[ReadyForIngestionRecord]): Seq[ReadyForIngestionRecord] = {
    if (!handleOnlyMaxInChunks) records
    else {
      def max(r1: ReadyForIngestionRecord, r2: ReadyForIngestionRecord): ReadyForIngestionRecord = {
        val r1Offset = r1.recordData.eventEnvelope.offset
        val r2Offset = r2.recordData.eventEnvelope.offset
        (r1Offset, r2Offset) match {
          case (NoOffset, NoOffset) => r1
          case (Sequence(_), NoOffset) => r1
          case (NoOffset, Sequence(_)) => r2
          case (Sequence(r1Seq), Sequence(r2Seq)) => if (r2Seq.compareTo(r1Seq) > 0) r2 else r1
        }
      }
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

  private def toTagEventData(recordAdapter: RecordAdapter): Option[RecordTagEventEnvelopData] = {
    Try {
      val newEvent = recordAdapter.getInternalObject.getDynamodb.getNewImage
      val persistentRepr = serializer.deserialize(ByteString(newEvent.get(journal.Payload).getB).toArray, classOf[PersistentRepr]).get
      val seqNumber = persistentRepr.sequenceNr
      val offset = Sequence(Try(recordAdapter.getSequenceNumber.toLong).getOrElse(0L))
      val tagged = persistentRepr.payload.asInstanceOf[Tagged]
      RecordTagEventEnvelopData(recordAdapter,
        tagged.tags.head,
        EventEnvelope(
          offset = offset,
          persistenceId = persistentRepr.persistenceId,
          sequenceNr = seqNumber,
          event = tagged.payload)
      )
    }.toOption
  }
}

case class DynamoDBTryRecordResult(record: RecordAdapter)
case class RecordTagEventEnvelopData(recordAdapter: RecordAdapter, tag: String, eventEnvelope: EventEnvelope)
case class ReadyForIngestionRecord(recordData: RecordTagEventEnvelopData, offsetQueuePair: OffsetQueuePair)
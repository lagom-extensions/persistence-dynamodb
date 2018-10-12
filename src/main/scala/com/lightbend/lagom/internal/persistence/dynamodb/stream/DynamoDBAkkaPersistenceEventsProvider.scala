package com.lightbend.lagom.internal.persistence.dynamodb.stream

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.dynamodb.dynamoClient
import akka.persistence.dynamodb.journal.{DynamoDBHelper, DynamoDBJournalConfig}
import akka.persistence.query.{EventEnvelope, Offset}
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream._
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBStreamsClientBuilder}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.lightbend.lagom.internal.persistence.dynamodb.DynamoDBPersistenceConfig._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * Stream data from DynamoDB akka persistence table and route to appropriate persistent entity sink
  * Forced to be object because
  * 1. There is no way to create a stream from dynamodb with filtering by tag
  * 2. Dedicated stream for each PersistentEntity will be extremely inefficient in receiving other tags events
  * 3. ReadJournalProvider creation is not under application code control, so no way of any dependency injection
  *
  * For attempt with guarantee delivery stream creation is delayed to give a chance to register all events by tag consumers
  * If there is a need to reapply all available dynamodb stream data, you can just change application name
  * this will be like new stream consumer for DynamoDB so events will start from first available(note: filtering by offset still occurs)
  *
  * For some very tricky case when you have to apply all events that no longer available as native DynamoDB streams
  * you can delegate all work to akka.persistence.dynamodb.journal.DynamoDBJournal
  *
  */
private[lagom] object DynamoDBAkkaPersistenceEventsProvider {
  private var systemWithJournalStreamARN: Option[(ActorSystem, String)] = None
  private val tagStreamSinks = new ConcurrentHashMap[String, OffsetQueuePair]()
  private var serializer: Serialization = _
  private var streamInitialized = false
  private var dynamo: DynamoDBHelper = _
  private val log = LoggerFactory.getLogger(getClass)

  type OffsetQueuePair = (Offset, SourceQueueWithComplete[EventEnvelope])

  def init(journalTable: TableDescription)(implicit system: ActorSystem): Unit = {
    try {
      val actualJournalTableName = journalTable.getTableName
      val streamARN = journalTable.getLatestStreamArn
      require(actualJournalTableName == journalTableName, s"Illegal try to start journal event streaming on table $actualJournalTableName expected table: $journalTableName")
      require(streamARN != null, s"No Streaming support on Journal table $journalTableName")
      systemWithJournalStreamARN = Some(system, streamARN)
      serializer = SerializationExtension(system)
    } catch {
      case ex: Exception =>
        log.error(s"Exception on initialization DynamoDB streaming, register termination. Exception: ${ex.getMessage}")
        system.terminate()
        throw ex
    }
  }

  def startStreaming(implicit system: ActorSystem): Unit = synchronized {
    if (streamInitialized) {
      log.warn("try to start DynamoDB Journal Streaming when it was already started")
    } else {
      systemWithJournalStreamARN match {
        case None =>
          throw new IllegalStateException("No journal table stream config, ensure call init first")
        case Some(_) if tagStreamSinks.isEmpty =>
          throw new IllegalStateException("No consumers, ensure consumer registered first")
        case Some((actorSystem, streamARN)) =>
          implicit val system: ActorSystem = actorSystem
          val journalSettings = new DynamoDBJournalConfig(dynamoDBJournalConfig)
          dynamo = dynamoClient(system, journalSettings)
          val kinesisConfig = new KinesisClientLibConfiguration(
            applicationName,
            streamARN,
            DefaultAWSCredentialsProviderChain.getInstance(),
            s"$applicationName-streams-worker-${UUID.randomUUID}"
          ).withMaxRecords(fetchDynamoDBMaxRecords)
            .withIdleTimeBetweenReadsInMillis(idleTimeBetweenReadsMills)
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
          val dynamoDBStreams = AmazonDynamoDBStreamsClientBuilder.standard()
            .withClientConfiguration(journalSettings.client.config)
            .withEndpointConfiguration(new EndpointConfiguration(journalTableEndpoint, dynamoDBRegion)).build()
          val workerThread = new Thread(new Worker.Builder()
            .recordProcessorFactory(new DynamoDBStreamActorEventsRecordProcessorFactory(dynamo.dynamoDB, serializer, system))
            .config(kinesisConfig)
            .kinesisClient(new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreams))
            .dynamoDBClient(dynamo.dynamoDB)
            .cloudWatchClient(AmazonCloudWatchClientBuilder.standard().withClientConfiguration(journalSettings.client.config)
              .withEndpointConfiguration(new EndpointConfiguration(cloudWatchEndpoint, cloudWatchRegion)).build()).build()
          )
          workerThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            override def uncaughtException(t: Thread, ex: Throwable): Unit = {
              log.error(s"Exception on working with DynamoDB streaming. Exception: ${ex.getMessage}")
            }
          })
          workerThread.start()
          streamInitialized = true
      }
    }
  }

  def eventsByTag(tag: String, offset: Offset)(implicit system: ActorSystem): Source[EventEnvelope, NotUsed] = {
    if (streamInitialized && tagStreamSinks.get(tag) == null) { // allow for case when ReadSideActor restarted
      throw new IllegalStateException("Stream already initialized ensure consumer registered first")
    }
    // TODO why exception not processed correctly got StreamDetachedException
    val queue = Source.queue[EventEnvelope](dynamoDBTagConsumerQueueBufferSize, OverflowStrategy.backpressure)
      .withAttributes(Attributes.logLevels(onFailure = Logging.DebugLevel))
      .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
    Source.fromGraph(queue)
      .mapMaterializedValue(sourceQueueWithComplete => {
        tagStreamSinks.put(tag, (offset, sourceQueueWithComplete))
        sourceQueueWithComplete.watchCompletion()
          .onComplete {
            case Success(_) =>
              log.error("Stream by tag Source queue completed, this should never happens, terminate actor system")
              system.terminate()
            case Failure(e) =>
              log.error("Stream by tag Source queue completed with exception, this should never happens, terminate actor system ", e)
              system.terminate()
          }(ExecutionContext.global)
        NotUsed
      })
  }

  private[stream] def getOffsetQueuePairByTag(tag: String): Option[OffsetQueuePair] = {
    Option(tagStreamSinks.get(tag))
  }

  private[stream] def updateOffsetForTag(tag: String, offset: Offset): Option[(Offset, SourceQueueWithComplete[EventEnvelope])] = {
    getOffsetQueuePairByTag(tag).map(previousOffsetQueuePair => tagStreamSinks.put(tag, (offset, previousOffsetQueuePair._2)))
  }

  def registeredConsumersCount: Int = tagStreamSinks.size

  def shutdown: Unit = if (dynamo != null) dynamo.shutdown()
}

private class DynamoDBStreamActorEventsRecordProcessorFactory(dynamoDBClient: AmazonDynamoDB, serializer: Serialization, system: ActorSystem) extends IRecordProcessorFactory {
  override def createProcessor: IRecordProcessor = new DynamoDBStreamActorEventsRecordProcessor(dynamoDBClient, serializer, system)
}
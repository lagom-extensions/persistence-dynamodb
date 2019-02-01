package com.lightbend.lagom.internal.persistence.dynamodb.stream

import java.net.URLEncoder
import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, ExtendedActorSystem, Props}
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.persistence.dynamodb.dynamoClient
import akka.persistence.dynamodb.journal.DynamoDBJournalConfig
import akka.persistence.journal.Tagged
import akka.persistence.query.{EventEnvelope, Offset, Sequence}
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import akka.util.{ByteString, Timeout}
import akka.{Done, NotUsed}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBStreams, AmazonDynamoDBStreamsClientBuilder}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.github.ghik.silencer.silent
import com.lightbend.lagom.internal.persistence.dynamodb.DynamoDBPersistenceConfig
import com.lightbend.lagom.internal.persistence.dynamodb.DynamoDBPersistenceConfig._

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try

/**
  * Stream data from DynamoDB akka persistence table and route to appropriate persistent entity sink
  * Forced to be cluster singleton because
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
  *
  */
private[lagom] class DynamoDBAkkaPersistenceEventsProviderActor(journalTable: TableDescription) extends Actor with ActorLogging {
  private val tagConsumers = mutable.Map[String, TagByOffsetConsumerActor]()
  implicit private val recordIngestionTimeout: Timeout = DynamoDBPersistenceConfig.readSideDynamoDBAwaitRecordIngestionTimeout(context.system)

  override def receive: Receive = {
    case StartDynamoDBStreaming =>
      log.info("Dynamo Streaming initialization started")
      startStreaming
      context become ready
    case AddDynamoDBStreamTagSubscriber(tag, offset, consumerActorSerializedPath) =>
      tagConsumers.put(tag, TagByOffsetConsumerActor(tag, offset, context.system.asInstanceOf[ExtendedActorSystem].provider.resolveActorRef(consumerActorSerializedPath)))
  }

  private def ready: Receive = {
    case StartDynamoDBStreaming =>
      log.info("Dynamo Streaming already initialized. This is valid on new cluster node start")
    case AddDynamoDBStreamTagSubscriber(tag, offset, consumerActorSerializedPath) =>
      log.warning("Late register DynamoDB consumer, this can cause missing events from DynamoDB. Ensure correct cluster ready time settings")
      tagConsumers.put(tag, TagByOffsetConsumerActor(tag, offset, context.system.asInstanceOf[ExtendedActorSystem].provider.resolveActorRef(consumerActorSerializedPath)))
  }

  @silent
  private[stream] def matchByOffset(fromOffset: Offset, recordOffset: Offset): Boolean = {
    true // AWS not valid sequence generation see https://github.com/aws/aws-sdk-java/issues/1798
    //    (fromOffset, recordOffset) match {
    //      case (NoOffset, _) => true
    //      case (Sequence(from), Sequence(actual)) => from < actual
    //      case _ => false
    //    }
  }

  private[stream] def handleEvent(item: ReadyForIngestionRecord): Future[Done] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val recordData = item.recordData
    val processingRecordOffset = recordData.offset
    val tagByOffsetConsumerActor = item.consumer
    val consumerActor = tagByOffsetConsumerActor.actorRef
    (consumerActor ask PublishEventMsg(recordData.offset, recordData.persistenceId, recordData.sequenceNr, recordData.serializedTaggedEventBytes))
      .map(
        _ =>
          tagConsumers
            .update(tagByOffsetConsumerActor.tag, TagByOffsetConsumerActor(tagByOffsetConsumerActor.tag, Some(processingRecordOffset), tagByOffsetConsumerActor.actorRef))
      )
      .map(_ => Done)
  }

  private[stream] def getConsumerActorByEventTag(tag: String): Option[TagByOffsetConsumerActor] = tagConsumers.get(tag)

  private def startStreaming(): Unit = {
    val actualJournalTableName = journalTable.getTableName
    val streamARN = journalTable.getLatestStreamArn
    require(
      actualJournalTableName == journalTableName(context.system),
      s"Illegal try to start journal event streaming on table $actualJournalTableName expected table: ${journalTableName(context.system)}"
    )
    require(streamARN != null, s"No Streaming support on Journal table ${journalTableName(context.system)}")
    implicit val system: ActorSystem = context.system
    val journalSettings = new DynamoDBJournalConfig(dynamoDBJournalConfig)
    val dynamo = dynamoClient(system, journalSettings)
    val kinesisConfig = new KinesisClientLibConfiguration(
      applicationName,
      streamARN,
      DefaultAWSCredentialsProviderChain.getInstance(),
      s"$applicationName-streams-worker-${UUID.randomUUID}"
    ).withMaxRecords(fetchDynamoDBMaxRecords)
      .withIdleTimeBetweenReadsInMillis(idleTimeBetweenReadsMills)
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

    val dynamoDBStreams: AmazonDynamoDBStreams = AmazonDynamoDBStreamsClientBuilder
      .standard()
      .withClientConfiguration(journalSettings.client.config)
      .withEndpointConfiguration(new EndpointConfiguration(journalStreamTableEndpoint, dynamoDBRegion))
      .build()

    val workerThread = new Thread(
      new Worker.Builder()
        .recordProcessorFactory(
          new DynamoDBStreamActorEventsRecordProcessorFactory(
            SerializationExtension(context.system),
            system,
            DynamoDBAkkaPersistenceEventsProviderActor.this
          )
        )
        .config(kinesisConfig)
        .kinesisClient(new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreams))
        .dynamoDBClient(dynamo.dynamoDB)
        .cloudWatchClient(
          AmazonCloudWatchClientBuilder
            .standard()
            .withClientConfiguration(journalSettings.client.config)
            .withEndpointConfiguration(new EndpointConfiguration(cloudWatchEndpoint, cloudWatchRegion))
            .build()
        )
        .build()
    )
    workerThread.setUncaughtExceptionHandler((_: Thread, ex: Throwable) => {
      log.error(s"Exception on working with DynamoDB streaming. Exception: ${ex.getMessage}")
    })
    workerThread.start()
  }
}

private[stream] case class TagByOffsetConsumerActor(tag: String, sequenceOffsetOpt: Option[Sequence], actorRef: ActorRef)

private[lagom] object DynamoDBAkkaPersistenceEventsProviderActor {
  def props(journalTable: TableDescription): Props = Props(new DynamoDBAkkaPersistenceEventsProviderActor(journalTable))
  val name = "dynamo-db-events-provider"

  @silent
  def eventsByTag(tag: String, offset: Offset)(implicit actorSystem: ActorSystem): Source[EventEnvelope, NotUsed] = {
    Source
      .actorPublisher[EventEnvelope](DynamoDBEventPublisher.props())
      .mapMaterializedValue(actorRef => {
        dynamoStreamingActorProxy ! AddDynamoDBStreamTagSubscriber(tag, sequenceOffsetOpt = offset match {
          case e: Sequence => Some(e)
          case _ => None
        }, serializedActorPath = Serialization.serializedActorPath(actorRef))
        NotUsed
      })
      .named("dynamoDBEventsByTag-" + URLEncoder.encode(tag, ByteString.UTF_8))
  }

  private[lagom] def dynamoStreamingActorProxy(implicit actorSystem: ActorSystem): ActorRef =
    actorSystem.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = s"/user/$name",
        settings = ClusterSingletonProxySettings(actorSystem)
          .withRole(
            Try(actorSystem.settings.config.getString("lagom.persistence.run-entities-on-role")).toOption.getOrElse("")
          )
      )
    )
}

@silent
private[lagom] class DynamoDBEventPublisher extends ActorPublisher[EventEnvelope] with ActorLogging {
  private val serializer = SerializationExtension(context.system)
  override def receive: Receive = {
    case msg: PublishEventMsg =>
      onNext(EventEnvelope(msg.offset, msg.persistenceId, msg.sequenceNr, serializer.deserialize(msg.serializedTaggedEventBytes, classOf[Tagged]).get.payload))
      sender ! Done
  }
}
private[lagom] object DynamoDBEventPublisher {
  def props(): Props = Props(new DynamoDBEventPublisher())
}

private class DynamoDBStreamActorEventsRecordProcessorFactory(
                                                               serializer: Serialization,
                                                               system: ActorSystem,
                                                               eventsProvider: DynamoDBAkkaPersistenceEventsProviderActor
                                                             ) extends IRecordProcessorFactory {
  override def createProcessor: IRecordProcessor =
    new DynamoDBStreamActorEventsRecordProcessor(serializer, system, eventsProvider)
}

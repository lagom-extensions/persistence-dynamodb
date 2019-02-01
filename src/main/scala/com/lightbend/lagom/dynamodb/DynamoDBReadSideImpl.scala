package com.lightbend.lagom.dynamodb

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import com.gu.scanamo.ops.ScanamoOps
import com.lightbend.lagom.dynamodb.DynamoDBReadSide.ReadSideHandlerBuilder
import com.lightbend.lagom.internal.persistence.dynamodb.DynamoDBOffsetStore
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.scaladsl.persistence.{AggregateEvent, AggregateEventTag, AggregateEventTagger, EventStreamElement}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Internal API
  */
private[lagom] final class DynamoDBReadSideImpl(system: ActorSystem,
                                                dynamoClient: DynamoClient,
                                                offsetStore: DynamoDBOffsetStore
                                               ) extends DynamoDBReadSide {

  private val dispatcher = system.settings.config.getString("lagom.persistence.read-side.use-dispatcher")
  implicit val ec = system.dispatchers.lookup(dispatcher)

  override def builder[Event <: AggregateEvent[Event]](eventProcessorId: String): ReadSideHandlerBuilder[Event] = {
    new ReadSideHandlerBuilder[Event] {

      import DynamoDBAutoReadSideHandler.Handler

      private var prepareCallback: AggregateEventTag[Event] => Future[Done] = (_: AggregateEventTagger[Event]) => Future.successful(Done)
      private var globalPrepareCallback: () => Future[Done] = () => Future.successful(Done)
      private var handlers = Map.empty[Class[_ <: Event], Handler[Event]]

      override def setGlobalPrepare(callback: () => Future[Done]): ReadSideHandlerBuilder[Event] = {
        globalPrepareCallback = callback
        this
      }

      override def setPrepare(callback: AggregateEventTag[Event] => Future[Done]): ReadSideHandlerBuilder[Event] = {
        prepareCallback = callback
        this
      }

      override def setEventHandler[E <: Event : ClassTag](handler: EventStreamElement[E] => Future[ScanamoOps[_]]): ReadSideHandlerBuilder[Event] = {
        val eventClass = implicitly[ClassTag[E]].runtimeClass.asInstanceOf[Class[Event]]
        handlers += (eventClass -> handler.asInstanceOf[Handler[Event]])
        this
      }

      override def build: ReadSideHandler[Event] = {
        new DynamoDBAutoReadSideHandler[Event](dynamoClient, offsetStore, handlers, globalPrepareCallback, prepareCallback, eventProcessorId, dispatcher)
      }
    }
  }
}

package com.lightbend.lagom.dynamodb

import akka.Done
import com.gu.scanamo.ops.ScanamoOps
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.scaladsl.persistence.{AggregateEvent, AggregateEventTag, EventStreamElement}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * DynamoDB read side support.
  *
  * This should be used to build and register readside
  */
object DynamoDBReadSide {

  /**
    * Builder for the handler.
    */
  trait ReadSideHandlerBuilder[Event <: AggregateEvent[Event]] {
    /**
      * Set a global prepare callback.
      *
      * @param callback The callback.
      * @return This builder for fluent invocation.
      * @see ReadSideHandler#globalPrepare()
      */
    def setGlobalPrepare(callback: () => Future[Done]): ReadSideHandlerBuilder[Event]

    /**
      * Set a prepare callback.
      *
      * @param callback The callback.
      * @return This builder for fluent invocation.
      * @see ReadSideHandler#prepare(AggregateEventTag)
      */
    def setPrepare(callback: AggregateEventTag[Event] => Future[Done]): ReadSideHandlerBuilder[Event]

    /**
      * Define the event handler that will be used for events of a given class.
      *
      * This variant allows for offsets to be consumed as well as their events.
      *
      * @tparam E The event type to handle.
      * @param handler The function to handle the events.
      * @return This builder for fluent invocation
      */
    def setEventHandler[E <: Event : ClassTag](handler: EventStreamElement[E] => Future[ScanamoOps[_]]): ReadSideHandlerBuilder[Event]

    /**
      * Build the read side handler.
      *
      * @return The read side handler.
      */
    def build: ReadSideHandler[Event]
  }

}

trait DynamoDBReadSide {
  /**
    * Create a builder for a DynamoDB read side event handler.
    *
    * @param readSideId An identifier for this read side. This will be used to store offsets in the offset store.
    * @return The builder.
    */
  def builder[Event <: AggregateEvent[Event]](readSideId: String): DynamoDBReadSide.ReadSideHandlerBuilder[Event]
}

package com.lightbend.lagom.dynamodb

import akka.persistence.query.Offset
import akka.stream.ActorAttributes
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import com.gu.scanamo.ScanamoAlpakka
import com.gu.scanamo.ops.ScanamoOps
import com.lightbend.lagom.internal.persistence.dynamodb.{DynamoDBOffsetDao, DynamoDBOffsetStore}
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.scaladsl.persistence._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Internal API
  */
private[dynamodb] abstract class DynamoDBReadSideHandler[Event <: AggregateEvent[Event]](dynamoClient: DynamoClient,
                                                                                         handlers: Map[Class[_ <: Event], DynamoDBAutoReadSideHandler.Handler[Event]],
                                                                                         dispatcher: String
                                                                                        )(implicit ec: ExecutionContext) extends ReadSideHandler[Event] {

  override def handle(): Flow[EventStreamElement[Event], Done, NotUsed] = {
    Flow[EventStreamElement[Event]]
      .mapAsync(parallelism = 1) { elem =>
        val eventClass = elem.event.getClass
        val offsetStoreOp = bindOffsetOps(elem)
        val optHandlerOp: Option[Future[ScanamoOps[_]]] = handlers.get(eventClass).map(_.apply(elem))
        optHandlerOp match {
          case None =>
            ScanamoAlpakka.exec(dynamoClient)(offsetStoreOp).map(_ => Done)
          case Some(futureOp) =>
            // todo generally can be batch, little bit confused now
            futureOp.flatMap(op => ScanamoAlpakka.exec(dynamoClient)(op))
              .flatMap(_ => ScanamoAlpakka.exec(dynamoClient)(offsetStoreOp))
              .map(_ => Done)
        }
      }.withAttributes(ActorAttributes.dispatcher(dispatcher))
  }

  protected def bindOffsetOps(event: EventStreamElement[Event]): ScanamoOps[_]
}

/**
  * Internal API
  */
private[dynamodb] object DynamoDBAutoReadSideHandler {
  type Handler[Event] = EventStreamElement[_ <: Event] => Future[ScanamoOps[_]]
}

/**
  * Internal API
  */
private[dynamodb] final class DynamoDBAutoReadSideHandler[Event <: AggregateEvent[Event]](dynamoClient: DynamoClient,
                                                                                          offsetStore: DynamoDBOffsetStore,
                                                                                          handlers: Map[Class[_ <: Event], DynamoDBAutoReadSideHandler.Handler[Event]],
                                                                                          globalPrepareCallback: () => Future[Done],
                                                                                          prepareCallback: AggregateEventTag[Event] => Future[Done],
                                                                                          readProcessorId: String,
                                                                                          dispatcher: String
                                                                                         )(implicit ec: ExecutionContext)
  extends DynamoDBReadSideHandler[Event](dynamoClient, handlers, dispatcher) {

  @volatile
  private var offsetDao: DynamoDBOffsetDao = _

  override protected def bindOffsetOps(element: EventStreamElement[Event]): ScanamoOps[_] =
    offsetDao.bindSaveOffset(element.offset)

  override def globalPrepare(): Future[Done] = globalPrepareCallback.apply()

  override def prepare(tag: AggregateEventTag[Event]): Future[Offset] = {
    for {
      _ <- prepareCallback.apply(tag)
      dao <- offsetStore.prepare(readProcessorId, tag.tag)
    } yield {
      offsetDao = dao
      dao.loadedOffset
    }
  }
}

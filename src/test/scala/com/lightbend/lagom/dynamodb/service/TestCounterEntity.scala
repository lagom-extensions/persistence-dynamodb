package com.lightbend.lagom.dynamodb.service

import java.math.BigDecimal.ZERO

import akka.persistence.journal.Tagged
import com.lightbend.lagom.scaladsl.persistence.PersistentEntity.ReplyType
import com.lightbend.lagom.scaladsl.persistence.{AggregateEvent, AggregateEventTag, PersistentEntity}
import com.lightbend.lagom.scaladsl.playjson.{JsonSerializer, JsonSerializerRegistry}
import play.api.libs.json.{Format, Json}

class TestCounterEntity extends PersistentEntity {
  override type Command = TestCounterCommand
  override type Event = TestCounterEvent
  override type State = Option[BigDecimal]

  override def initialState: Option[BigDecimal] = None

  override def entityTypeName: String = "counter-entity"

  override def behavior: Behavior =
    Actions()
      .onCommand[TestIncrementTestCounterCmd, TestCounterState] {
        case (TestIncrementTestCounterCmd(amount), ctx, state) =>
          ctx.thenPersist(
            TestCounterUpdatedEvent(
              amount.bigDecimal,
              state.getOrElse(ZERO),
              amount + state.getOrElse(ZERO)
            )
          ) { evt => ctx.reply(TestCounterState(entityId, evt.nextState))
          }
      }
      .onEvent(
        // todo can be removed, track https://github.com/lagom/lagom/issues/1611
        new PartialFunction[(TestCounterEvent, State), State] {
          override def isDefinedAt(x: (TestCounterEvent, Option[BigDecimal])): Boolean = {
            x._1.isInstanceOf[Tagged] || x._1.isInstanceOf[TestCounterEvent]
          }
          override def apply(v1: (TestCounterEvent, Option[BigDecimal])): Option[BigDecimal] = {
            if (v1._1.isInstanceOf[Tagged]) {
              val tagged = v1._1.asInstanceOf[Tagged]
              Some(tagged.payload.asInstanceOf[TestCounterUpdatedEvent].nextState)
            } else {
              Some(v1._1.asInstanceOf[TestCounterUpdatedEvent].nextState)
            }
          }
        }
      )
      .onReadOnlyCommand[TestGetCounterStateCmd, TestCounterState] {
        case (TestGetCounterStateCmd(), ctx, state) =>
          ctx.reply(TestCounterState(entityId, state.getOrElse(BigDecimal(ZERO))))
      }
}

sealed trait TestCounterCommand extends TestRemoting
case class TestIncrementTestCounterCmd(amount: BigDecimal) extends TestCounterCommand with ReplyType[TestCounterState]
case class TestGetCounterStateCmd() extends TestCounterCommand with ReplyType[TestCounterState]

sealed trait TestCounterEvent extends AggregateEvent[TestCounterEvent] {
  override def aggregateTag = TestCounterEvent.Tag
}
object TestCounterEvent {
  val NumShards = 10
  val Tag = AggregateEventTag.sharded[TestCounterEvent](NumShards)
}
case class TestCounterUpdatedEvent(amount: BigDecimal, previousState: BigDecimal, nextState: BigDecimal)
    extends TestCounterEvent
object TestCounterUpdatedEvent {
  implicit val format: Format[TestCounterUpdatedEvent] = Json.format
}

object TestCounterSerializerRegistry extends JsonSerializerRegistry {
  override def serializers = List(JsonSerializer[TestCounterUpdatedEvent])
}

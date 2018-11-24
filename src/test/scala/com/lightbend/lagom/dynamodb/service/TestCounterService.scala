package com.lightbend.lagom.dynamodb.service

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.{Service, ServiceCall}
import play.api.libs.json._

trait TestCounterService extends Service {
  def incrementCounter: ServiceCall[TestIncrementCounter, TestCounterState]
  def getCounterState(counterName: String): ServiceCall[NotUsed, TestCounterState]
  def getAllCounters: ServiceCall[NotUsed, TestCounterStatistic]

  def descriptor = {
    import Service._
    named("counter").withCalls(
      pathCall("/api/counter", incrementCounter),
      pathCall("/api/counter/:counterName", getCounterState _),
      pathCall("/api/counter", getAllCounters)
    )
  }
}

case class TestIncrementCounter(counter: String, amount: BigDecimal)
object TestIncrementCounter {
  implicit val format: Format[TestIncrementCounter] = Json.format
}

case class TestGetCounterState(counter: String)
object TestGetCounterState {
  implicit val format: Format[TestGetCounterState] = Json.format
}

case class TestCounterState(counter: String, amount: BigDecimal)
object TestCounterState {
  implicit val format: Format[TestCounterState] = Json.format
}

case class TestCounterStatistic(counterData: Seq[TestCounterState])
object TestCounterStatistic {
  implicit val format: Format[TestCounterStatistic] = Json.format
}

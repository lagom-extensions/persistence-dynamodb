package com.lightbend.lagom.dynamodb

import java.math.BigDecimal.ZERO
import java.util.UUID

import akka.actor.ActorSystem
import akka.actor.setup.ActorSystemSetup
import akka.testkit.TestKit
import com.github.ghik.silencer.silent
import com.lightbend.lagom.dynamodb.service._
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.lightbend.lagom.scaladsl.testkit.PersistentEntityTestDriver
import org.scalatest._

import scala.collection.immutable.Nil

@silent
class TestCounterEntitySpec extends WordSpec with Matchers with BeforeAndAfterAll with OptionValues {
  private val system = ActorSystem("test", ActorSystemSetup(JsonSerializerRegistry.serializationSetupFor(TestCounterSerializerRegistry)))

  override def afterAll = TestKit.shutdownActorSystem(system)

  private def withDriver[T](entityId: UUID)(block: PersistentEntityTestDriver[TestCounterCommand, TestCounterEvent, Option[BigDecimal]] => T): T = {
    val driver = new PersistentEntityTestDriver(system, new TestCounterEntity, entityId.toString)
    try {
      block(driver)
    } finally {
      driver.getAllIssues shouldBe empty
    }
  }

  "The counter entity " should {
    "increment counter that don't have previous state" in withDriver(UUID.randomUUID) { driver =>
      val outcome = driver.run(TestIncrementCounterCmd(BigDecimal(1.11)))
      outcome.events should contain only TestCounterUpdatedEvent(
        BigDecimal(1.11),
        BigDecimal(ZERO),
        BigDecimal(1.11)
      )
      outcome.state should ===(Some(BigDecimal(1.11)))
    }

    "increment counter that already have state" in withDriver(UUID.randomUUID) { driver =>
      val initOut = driver.run(TestIncrementCounterCmd(BigDecimal(1.11)))
      val outcome = driver.run(TestIncrementCounterCmd(BigDecimal(1.11)))
      outcome.events should contain only TestCounterUpdatedEvent(
        BigDecimal(1.11),
        BigDecimal(1.11),
        BigDecimal(2.22)
      )
      outcome.state should ===(Some(BigDecimal(2.22)))
    }

    "response with empty state on no state" in withDriver(UUID.randomUUID) { driver =>
      val outcome = driver.run(TestGetCounterStateCmd())
      outcome.events should ===(Nil)
      outcome.state should ===(None)
    }

    "response with state value" in withDriver(UUID.randomUUID) { driver =>
      val initOut = driver.run(TestIncrementCounterCmd(BigDecimal(1.11)))
      val outcome = driver.run(TestGetCounterStateCmd())
      outcome.events should ===(Nil)
      outcome.state should ===(Some(BigDecimal(1.11)))
    }
  }
}

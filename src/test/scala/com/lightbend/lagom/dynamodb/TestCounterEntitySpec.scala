package com.lightbend.lagom.dynamodb

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.lightbend.lagom.dynamodb.service._
import com.lightbend.lagom.scaladsl.testkit.PersistentEntityTestDriver
import org.scalatest._

import scala.collection.immutable.Nil

class TestCounterEntitySpec
    extends WordSpec
    with Matchers
    with BeforeAndAfterAll
    with OptionValues {
  private val system = ActorSystem("test")

  val sZERO = BigDecimal(java.math.BigDecimal.ZERO)
  override def afterAll = {
    TestKit.shutdownActorSystem(system)
  }

  private val counterName = UUID.randomUUID

  private def withDriver[T](
      block: PersistentEntityTestDriver[TestCounterCommand, TestCounterEvent, Option[
        BigDecimal
      ]] => T
  ): T = {
    val driver = new PersistentEntityTestDriver(
      system,
      new TestCounterEntity,
      counterName.toString
    )
    try {
      block(driver)
    } finally {
      driver.getAllIssues shouldBe empty
    }
  }

  "The counter entity " should {
    "increment counter that don't have previous state" in withDriver { driver =>
      val outcome = driver.run(TestIncrementTestCounterCmd(BigDecimal(1.11)))
      outcome.events should contain only TestCounterUpdatedEvent(
        BigDecimal(1.11),
        sZERO,
        sZERO
      )
      outcome.state should ===(Some(BigDecimal(1.11)))
    }

    "increment counter that already have state" in withDriver { driver =>
      val initOut = driver.run(TestIncrementTestCounterCmd(BigDecimal(1.11)))
      val outcome = driver.run(TestIncrementTestCounterCmd(BigDecimal(1.11)))
      outcome.events should contain only TestCounterUpdatedEvent(
        BigDecimal(1.11),
        sZERO,
        sZERO
      )
      outcome.state should ===(Some(BigDecimal(2.22)))
    }

    "response with empty state on no state" in withDriver { driver =>
      val outcome = driver.run(TestGetCounterStateCmd())
      outcome.events should ===(Nil)
      outcome.state should ===(None)
    }

    "response with state value" in withDriver { driver =>
      val initOut = driver.run(TestIncrementTestCounterCmd(BigDecimal(1.11)))
      val outcome = driver.run(TestGetCounterStateCmd())
      outcome.events should ===(Nil)
      outcome.state should ===(Some(BigDecimal(1.11)))
    }
  }
}

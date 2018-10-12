package com.lightbend.lagom.dynamodb

import java.util.UUID

import com.lightbend.lagom.dynamodb.service.{TestCounterComponents, TestCounterService, TestCounterState, TestIncrementCounter}
import com.lightbend.lagom.scaladsl.server.{LagomApplication, LocalServiceLocator}
import com.lightbend.lagom.scaladsl.testkit.ServiceTest
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Matchers}
import play.api.libs.ws.ahc.AhcWSComponents

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Future, Promise}

class TestCounterServiceITest extends AsyncWordSpec with BeforeAndAfterAll with Matchers {

  private val server = ServiceTest.startServer(ServiceTest.defaultSetup) { ctx =>
    new LagomApplication(ctx) with LocalServiceLocator with TestCounterComponents with AhcWSComponents
  }

  def actorSystem = server.application.actorSystem

  override def afterAll() = server.stop()

  val counterService = server.serviceClient.implement[TestCounterService]

  "The counter event processor" should {
    "create counter state data" in {
      val counterId = UUID.randomUUID.toString
      (for {
        _ <- feed(counterId, BigDecimal(1.11))
      } yield {
        awaitSuccess() {
          for {
            counters <- getCounters()
          } yield {
            counters should contain(TestCounterState(counterId, BigDecimal(1.11)))
          }
        }
      }).flatMap(identity)
    }

    "update counter state data" in {
      val counterId = UUID.randomUUID.toString
      (for {
        _ <- feed(counterId, BigDecimal(1.11))
        _ <- feed(counterId, BigDecimal(1.11))
        _ <- feed(counterId, BigDecimal(0))
        _ <- feed(counterId, BigDecimal(0))
        _ <- feed(counterId, BigDecimal(0))
        _ <- feed(counterId, BigDecimal(1))
        _ <- feed(counterId, BigDecimal(-1))
      } yield {
        awaitSuccess() {
          for {
            counters <- getCounters()
          } yield {
            counters should contain(TestCounterState(counterId, BigDecimal(2.22)))
          }
        }
      }).flatMap(identity)
    }

    "create and update counter state data" in {
      val counterId_1 = UUID.randomUUID.toString
      val counterId_2 = UUID.randomUUID.toString
      (for {
        _ <- feed(counterId_1, BigDecimal(1.11))
        _ <- feed(counterId_2, BigDecimal(1.11))
        _ <- feed(counterId_2, BigDecimal(1.11))
      } yield {
        awaitSuccess() {
          for {
            counters <- getCounters()
          } yield {
            counters should contain {
              TestCounterState(counterId_1, BigDecimal(1.11))
              TestCounterState(counterId_2, BigDecimal(2.22))
            }
          }
        }
      }).flatMap(identity)
    }
  }

  private def getCounters() = {
    counterService.getAllCounters.invoke().map(_.counterData)
  }

  private def feed(counterId: String, amount: BigDecimal) = {
    counterService.incrementCounter.invoke(TestIncrementCounter(counterId, amount))
  }

  def awaitSuccess[T](
      maxDuration: FiniteDuration = 120.seconds,
      checkEvery: FiniteDuration = 1.second
  )(block: => Future[T]): Future[T] = {
    val checkUntil = System.currentTimeMillis() + maxDuration.toMillis

    def doCheck(): Future[T] = {
      block.recoverWith {
        case recheck if checkUntil > System.currentTimeMillis() =>
          val timeout = Promise[T]()
          actorSystem.scheduler.scheduleOnce(checkEvery) {
            timeout.completeWith(doCheck())
          }(actorSystem.dispatcher)
          timeout.future
        case recheckTimeFailed =>
          throw new AssertionError("failed await success")
      }
    }
    doCheck()
  }
}

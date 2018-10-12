package com.lightbend.lagom.dynamodb.service

import akka.NotUsed
import com.lightbend.lagom.dynamodb.service.dao.TestCounterStateRepository
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.lightbend.lagom.scaladsl.persistence.PersistentEntityRegistry

import scala.concurrent.ExecutionContext

class TestCounterServiceImpl(
    counterStateRepository: TestCounterStateRepository,
    registry: PersistentEntityRegistry
)(implicit ec: ExecutionContext)
    extends TestCounterService {
  override def incrementCounter: ServiceCall[TestIncrementCounter, TestCounterState] = ServiceCall { incrementCmd =>
    refFor(incrementCmd.counter).ask(TestIncrementTestCounterCmd(incrementCmd.amount))
  }

  override def getCounterState(counterName: String) = ServiceCall { _ => refFor(counterName).ask(TestGetCounterStateCmd())
  }

  override def getAllCounters: ServiceCall[NotUsed, TestCounterStatistic] = ServiceCall { _ =>
    counterStateRepository.getAllCounters()
  }

  private def refFor(counterName: String) =
    registry.refFor[TestCounterEntity](counterName)

}

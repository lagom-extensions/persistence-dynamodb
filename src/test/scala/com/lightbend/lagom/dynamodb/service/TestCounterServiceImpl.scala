package com.lightbend.lagom.dynamodb.service

import akka.NotUsed
import com.lightbend.lagom.dynamodb.service.dao.TestCounterStateRepository
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.lightbend.lagom.scaladsl.persistence.PersistentEntityRegistry

class TestCounterServiceImpl(
    counterStateRepository: TestCounterStateRepository,
    registry: PersistentEntityRegistry
) extends TestCounterService {
  override def incrementCounter: ServiceCall[TestIncrementCounter, TestCounterState] = ServiceCall { incrementCmd =>
    refFor(incrementCmd.counter).ask(TestIncrementCounterCmd(incrementCmd.amount))
  }

  override def getCounterState(counterName: String) = ServiceCall { _ =>
    refFor(counterName).ask(TestGetCounterStateCmd())
  }

  override def getAllCounters: ServiceCall[NotUsed, TestCounterStatistic] = ServiceCall { _ =>
    counterStateRepository.getAllCounters()
  }

  private def refFor(counterName: String) =
    registry.refFor[TestCounterEntity](counterName)

}

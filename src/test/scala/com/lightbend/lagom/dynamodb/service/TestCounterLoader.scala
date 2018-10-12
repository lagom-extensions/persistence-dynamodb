package com.lightbend.lagom.dynamodb.service

import akka.stream.Materializer
import com.lightbend.lagom.dynamodb.DynamoDBPersistenceComponents
import com.lightbend.lagom.dynamodb.service.dao.{CounterEventProcessor, TestCounterStateRepository}
import com.lightbend.lagom.scaladsl.client.ConfigurationServiceLocatorComponents
import com.lightbend.lagom.scaladsl.devmode.LagomDevModeComponents
import com.lightbend.lagom.scaladsl.server._
import com.softwaremill.macwire._
import play.api.libs.ws.ahc.AhcWSComponents

import scala.concurrent.ExecutionContext

trait TestCounterComponents extends LagomServerComponents with DynamoDBPersistenceComponents {

  implicit def executionContext: ExecutionContext
  implicit def materializer: Materializer

  override lazy val lagomServer = serverFor[TestCounterService](wire[TestCounterServiceImpl])
  override lazy val jsonSerializerRegistry = TestCounterSerializerRegistry

  implicit lazy val appCounterSettings = wire[TestAppCounterSettings]

  lazy val counterStateRepository = wire[TestCounterStateRepository]
  lazy val counterEventProcessor = wire[CounterEventProcessor]
  readSide.register(counterEventProcessor)
  persistentEntityRegistry.register(wire[TestCounterEntity])
  override lazy val readSideTags = TestCounterEvent.Tag.allTags.map(_.tag)
}

abstract class TestCounterApplication(context: LagomApplicationContext)
    extends LagomApplication(context)
    with TestCounterComponents
    with AhcWSComponents {}

class TestCounterLoader extends LagomApplicationLoader {
  override def load(context: LagomApplicationContext) =
    new TestCounterApplication(context) with ConfigurationServiceLocatorComponents

  override def loadDevMode(context: LagomApplicationContext) =
    new TestCounterApplication(context) with LagomDevModeComponents

  override def describeService = Some(readDescriptor[TestCounterService])
}

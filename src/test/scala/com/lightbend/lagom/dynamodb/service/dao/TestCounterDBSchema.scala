package com.lightbend.lagom.dynamodb.service.dao

import com.gu.scanamo.Table
import com.lightbend.lagom.dynamodb.service.{TestAppCounterSettings, TestCounterState}

trait TestCounterDBSchema {
  def appCounterSettings: TestAppCounterSettings
  val counterStateTable = Table[TestCounterState](appCounterSettings.TestCounterStateTableName)
}

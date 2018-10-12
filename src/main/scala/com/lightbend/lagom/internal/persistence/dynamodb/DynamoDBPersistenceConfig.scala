package com.lightbend.lagom.internal.persistence.dynamodb

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

private[lagom] object DynamoDBPersistenceConfig {

  def applicationName(implicit system: ActorSystem): String = {
    val config = system.settings.config
    config.getString("lagom.persistence.application-name")
  }

  def globalPrepareTimeout(implicit system: ActorSystem): Long = system.settings.config.getDuration("lagom.persistence.read-side.global-prepare-timeout", TimeUnit.MILLISECONDS)
  def readSideProcessOnlyMaxByOffset(implicit system: ActorSystem): Boolean = system.settings.config.getBoolean("lagom.persistence.read-side.dynamodb.process-only-max-by-offset")
  def readSideDynamoDBAwaitRecordIngestionTimeout(implicit system: ActorSystem): FiniteDuration = FiniteDuration(system.settings.config.getDuration("lagom.persistence.read-side.dynamodb.await-ingestion-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  def fetchDynamoDBMaxRecords(implicit system: ActorSystem): Int = system.settings.config.getInt("lagom.persistence.dynamodb.fetch-max-records")
  def dynamoDBTagConsumerQueueBufferSize(implicit system: ActorSystem): Int = system.settings.config.getInt("lagom.persistence.dynamodb.tag-consumer-queue-buffer-size")
  def idleTimeBetweenReadsMills(implicit system: ActorSystem): Int = system.settings.config.getInt("lagom.persistence.dynamodb.idle-time-between-reads-mills")

  def cloudWatchEndpoint(implicit system: ActorSystem): String = system.settings.config.getString("lagom.persistence.dynamodb.cloud-watch.endpoint")
  def cloudWatchRegion(implicit system: ActorSystem): String = system.settings.config.getString("lagom.persistence.dynamodb.cloud-watch.region")

  def dynamoDBRegion(implicit system: ActorSystem): String = system.settings.config.getString("lagom.persistence.dynamodb.region")
  def journalTableEndpoint(implicit system: ActorSystem): String = persistenceConfig(JOURNAL_PLUGIN_CONFIG).getString("endpoint")
  def journalTableName(implicit system: ActorSystem): String = persistenceConfig(JOURNAL_PLUGIN_CONFIG).getString("journal-table")
  def journalReadCapacityUnits(implicit system: ActorSystem): Long = system.settings.config.getInt("lagom.persistence.dynamodb.table.journal.read-capacity-units")
  def journalWriteCapacityUnits(implicit system: ActorSystem): Long = system.settings.config.getInt("lagom.persistence.dynamodb.table.journal.write-capacity-units")

  def snapshotTableName(implicit system: ActorSystem): String = persistenceConfig(SNAPSHOT_PLUGIN_CONFIG).getString("snapshot-table")
  def snapshotReadCapacityUnits(implicit system: ActorSystem): Long = system.settings.config.getInt("lagom.persistence.dynamodb.table.snapshot.read-capacity-units")
  def snapshotWriteCapacityUnits(implicit system: ActorSystem): Long = system.settings.config.getInt("lagom.persistence.dynamodb.table.snapshot.write-capacity-units")

  private def persistenceConfig(confType: AkkaPersistenceConfig)(implicit system: ActorSystem): Config = {
    val config = system.settings.config
    if (!config.hasPath(confType.confPath)) throw logNotValidConfigAndTerminate
    else config.getConfig(config.getString(confType.confPath))
  }

  def dynamoDBJournalConfig(implicit system: ActorSystem): Config = persistenceConfig(JOURNAL_PLUGIN_CONFIG)

  def validateDynamoPersistenceConfig(implicit system: ActorSystem): Unit = {
    def validateConfig(confType: AkkaPersistenceConfig, pluginClass: String): Unit = {
      val config = persistenceConfig(confType)
      if (!config.hasPathOrNull("class") || !config.getString("class").startsWith(pluginClass))
        logNotValidConfigAndTerminate
    }

    validateConfig(JOURNAL_PLUGIN_CONFIG, "akka.persistence.dynamodb.journal.")
    validateConfig(SNAPSHOT_PLUGIN_CONFIG, "akka.persistence.dynamodb.snapshot.")
  }

  private def logNotValidConfigAndTerminate(implicit system: ActorSystem): Exception = {
    system.log.error("Not valid configuration for dynamodb persistence actors")
    system.terminate()
    throw new IllegalStateException("Not valid configuration for dynamodb persistence actors")
  }
}

private sealed trait AkkaPersistenceConfig {
  val confPath: String
}
private case object JOURNAL_PLUGIN_CONFIG extends AkkaPersistenceConfig {
  override val confPath = "akka.persistence.journal.plugin"
}
private case object SNAPSHOT_PLUGIN_CONFIG extends AkkaPersistenceConfig {
  override val confPath = "akka.persistence.snapshot-store.plugin"
}
package com.lightbend.lagom.internal.persistence.dynamodb

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.query.javadsl.{EventsByTagQuery => JEventsByTagQuery, ReadJournal => JReadJournal}
import akka.persistence.query.scaladsl.{EventsByTagQuery, ReadJournal}
import akka.persistence.query.{EventEnvelope, Offset, ReadJournalProvider}
import akka.stream.javadsl.{Source => JSource}
import akka.stream.scaladsl.Source
import com.github.ghik.silencer.silent
import com.lightbend.lagom.internal.persistence.dynamodb.stream.DynamoDBAkkaPersistenceEventsProviderActor
import com.typesafe.config.Config

@silent
class DynamoDBReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {
  override val scaladslReadJournal: ScalaDSLDynamoDBReadJournal = new ScalaDSLDynamoDBReadJournal(system)
  override val javadslReadJournal: JavaDSLDynamoDBReadJournal = new JavaDSLDynamoDBReadJournal(system)
}
private[lagom] class ScalaDSLDynamoDBReadJournal(system: ExtendedActorSystem) extends EventsByTagQuery with ReadJournal {
  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] =
    DynamoDBAkkaPersistenceEventsProviderActor.eventsByTag(tag, offset)(system)
}
private[lagom] class JavaDSLDynamoDBReadJournal(system: ExtendedActorSystem) extends JEventsByTagQuery with JReadJournal {
  override def eventsByTag(tag: String, offset: Offset): JSource[EventEnvelope, NotUsed] =
    DynamoDBAkkaPersistenceEventsProviderActor.eventsByTag(tag, offset)(system).asJava
}

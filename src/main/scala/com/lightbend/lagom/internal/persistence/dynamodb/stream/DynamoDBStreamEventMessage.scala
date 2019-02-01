package com.lightbend.lagom.internal.persistence.dynamodb.stream

import akka.persistence.query._
import com.lightbend.lagom.scaladsl.playjson.{JsonSerializer, JsonSerializerRegistry}
import play.api.libs.json._

private[lagom] case class StartDynamoDBStreaming()
private[lagom] object StartDynamoDBStreaming {
  implicit val strictReads = Reads[StartDynamoDBStreaming](json => json.validate[JsObject].filter(_.values.isEmpty).map(_ => StartDynamoDBStreaming()))
  implicit val writes = OWrites[StartDynamoDBStreaming](_ => Json.obj())
}

private[stream] case class AddDynamoDBStreamTagSubscriber(tag: String, sequenceOffsetOpt: Option[Sequence], serializedActorPath: String)
private[stream] object AddDynamoDBStreamTagSubscriber {
  implicit val seqOffsetFormat: Format[Sequence] = Json.format
  implicit val format: Format[AddDynamoDBStreamTagSubscriber] = Json.format
}

private[stream] case class PublishEventMsg(offset: Sequence, persistenceId: String, sequenceNr: Long, serializedTaggedEventBytes: Array[Byte])
private[stream] object PublishEventMsg {
  implicit val seqOffsetFormat: Format[Sequence] = Json.format
  implicit val format: Format[PublishEventMsg] = Json.format
}

private[lagom] object DynamoDBSerializerRegistry extends JsonSerializerRegistry {
  override def serializers = List(
    JsonSerializer[StartDynamoDBStreaming],
    JsonSerializer[PublishEventMsg],
    JsonSerializer[AddDynamoDBStreamTagSubscriber]
  )
}

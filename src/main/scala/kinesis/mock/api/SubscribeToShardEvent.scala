package kinesis.mock.api

import io.circe._

import kinesis.mock.models._

final case class SubscribeToShardEvent(
    childShards: Option[List[ChildShard]],
    continuationSequenceNumber: SequenceNumber,
    millisBehindLatest: Long,
    records: List[KinesisRecord]
)

object SubscribeToShardEvent {
  implicit val subscribeToShardEventCirceEncoder
      : Encoder[SubscribeToShardEvent] = Encoder.forProduct4(
    "ChildShards",
    "ContinuationSequenceNumber",
    "MillisBehindLatest",
    "Records"
  )(x =>
    (
      x.childShards,
      x.continuationSequenceNumber,
      x.millisBehindLatest,
      x.records
    )
  )

  implicit val subscribeToShardEventCirceDecoder
      : Decoder[SubscribeToShardEvent] = x =>
    for {
      childShards <- x.downField("ChildShards").as[Option[List[ChildShard]]]
      continuationSequenceNumber <- x
        .downField("ContinuationSequenceNumber")
        .as[SequenceNumber]
      millisBehindLatest <- x.downField("MillisBehindLatest").as[Long]
      records <- x.downField("Records").as[List[KinesisRecord]]
    } yield SubscribeToShardEvent(
      childShards,
      continuationSequenceNumber,
      millisBehindLatest,
      records
    )
}

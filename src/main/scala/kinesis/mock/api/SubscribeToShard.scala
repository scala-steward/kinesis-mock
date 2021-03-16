package kinesis.mock
package api

import fs2.Stream
import io.circe._
import kinesis.mock.models.Streams
import cats.effect.concurrent.Ref
import cats.effect.IO
import cats.data._
import cats.syntax.all._
import cats.data.Validated._
import kinesis.mock.models._

// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShard.html
final case class SubscribeToShard(
    consumerArn: String,
    shardId: String,
    startingPosition: StartingPosition
) {
  def getRecords(
      streams: Streams,
      limit: Option[Int],
      newStartingPosition: Option[StartingPosition]
  ): ValidatedNel[
    KinesisMockException,
    (SubscribeToShardEvent, StartingPosition)
  ] =
    CommonValidations.findStreamByConsumerArn(consumerArn, streams).andThen {
      case (consumer, stream) =>
        if (consumer.consumerStatus != ConsumerStatus.ACTIVE)
          ResourceInUseException(
            s"Consumer ${consumer.consumerName} is not currently ACTIVE"
          ).invalidNel
        else if (
          stream.streamStatus != StreamStatus.ACTIVE && stream.streamStatus != StreamStatus.UPDATING
        )
          ResourceInUseException(
            s"Stream ${stream.streamName} is not currently ACTIVE or UPDATING"
          ).invalidNel
        else
          (
            CommonValidations.validateShardId(shardId),
            CommonValidations.findShard(shardId, stream).andThen {
              case (shard, data) =>
                (
                  CommonValidations.isShardOpen(shard),
                  newStartingPosition
                    .getOrElse(startingPosition)
                    .validateInitialRecords(data, shard, limit)
                ).mapN {
                  case (_, (initialRecords, continuationSequenceNumber)) =>
                    (shard, data, initialRecords, continuationSequenceNumber)
                }
            }
          ).mapN {
            case (
                  _,
                  (shard, data, initialRecords, continuationSequenceNumber)
                ) =>
              val allShards = stream.shards.keys.toList
              val childShards = allShards
                .filter(_.parentShardId.contains(shard.shardId))
                .map(s =>
                  ChildShard.fromShard(
                    s,
                    allShards
                      .filter(
                        _.parentShardId.contains(s.shardId)
                      )
                  )
                )
              (
                SubscribeToShardEvent(
                  if (childShards.nonEmpty) Some(childShards) else None,
                  continuationSequenceNumber,
                  initialRecords.headOption
                    .map(record =>
                      data.last.approximateArrivalTimestamp.toEpochMilli -
                        record.approximateArrivalTimestamp
                          .toEpochMilli()
                    )
                    .getOrElse(0L),
                  List.empty
                ),
                StartingPosition(
                  Some(continuationSequenceNumber),
                  None,
                  ShardIteratorType.AFTER_SEQUENCE_NUMBER
                )
              )
          }
    }

  def subscribeToShard(
      streamsRef: Ref[IO, Streams],
      limit: Option[Int]
  ): IO[
    ValidatedNel[KinesisMockException, Stream[IO, SubscribeToShardEvent]]
  ] = {
    streamsRef.get
      .map(streams => getRecords(streams, limit, None))
      .map(_.map { case (initial, _) =>
        Stream.emit(initial)
      })
  }
}

object SubscribeToShard {
  implicit val subscribeToShardCirceEncoder: Encoder[SubscribeToShard] =
    Encoder.forProduct3("ConsumerARN", "ShardId", "StartingPosition")(x =>
      (x.consumerArn, x.shardId, x.startingPosition)
    )

  implicit val subscribeToShardCirceDecoder: Decoder[SubscribeToShard] = x =>
    for {
      consumerArn <- x.downField("ConsumerARN").as[String]
      shardId <- x.downField("ShardId").as[String]
      startingPosition <- x.downField("StartingPosition").as[StartingPosition]
    } yield SubscribeToShard(consumerArn, shardId, startingPosition)
}

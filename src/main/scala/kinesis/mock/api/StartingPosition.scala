package kinesis.mock
package api

import kinesis.mock.models.SequenceNumber
import java.time.Instant

import cats.data._
import cats.data.Validated._
import cats.syntax.all._
import io.circe._
import kinesis.mock.models._

final case class StartingPosition(
    sequenceNumber: Option[SequenceNumber],
    timestamp: Option[Instant],
    `type`: ShardIteratorType
) {
  def validateInitialRecords(
      data: List[KinesisRecord],
      shard: Shard,
      limit: Option[Int]
  ): ValidatedNel[
    KinesisMockException,
    (List[KinesisRecord], SequenceNumber)
  ] = {
    val now = Instant.now()
    val maxRecords = limit.getOrElse(10000)
    if (data.isEmpty)
      Valid((List.empty, shard.sequenceNumberRange.startingSequenceNumber))
    else
      (`type`, sequenceNumber, timestamp) match {
        case (ShardIteratorType.AT_SEQUENCE_NUMBER, None, _) |
            (ShardIteratorType.AFTER_SEQUENCE_NUMBER, None, _) =>
          InvalidArgumentException(
            s"SequenceNumber must be provided for ShardIteratorType ${`type`}"
          ).invalidNel
        case (ShardIteratorType.AT_TIMESTAMP, _, None) =>
          InvalidArgumentException(
            s"Timestamp must be provided for ShardIteratorType ${`type`}"
          ).invalidNel
        case (ShardIteratorType.AT_TIMESTAMP, _, Some(ts))
            if (ts.toEpochMilli() < now.toEpochMilli()) =>
          InvalidArgumentException(
            "Timestamp cannot be in the future"
          ).invalidNel
        case (ShardIteratorType.AT_SEQUENCE_NUMBER, Some(seqNo), _)
            if (data.find(_.sequenceNumber == seqNo).isEmpty) =>
          ResourceNotFoundException(
            s"Unable to find record with provided SequenceNumber"
          ).invalidNel
        case (ShardIteratorType.AFTER_SEQUENCE_NUMBER, Some(seqNo), _)
            if (data.find(_.sequenceNumber == seqNo).isEmpty) =>
          ResourceNotFoundException(
            s"Unable to find record with provided SequenceNumber"
          ).invalidNel
        case (ShardIteratorType.LATEST, _, _) =>
          Valid((List.empty, data.last.sequenceNumber))
        case (ShardIteratorType.TRIM_HORIZON, _, _) =>
          val recs = data.take(maxRecords)
          Valid(
            (recs, recs.last.sequenceNumber)
          )
        case (ShardIteratorType.AT_TIMESTAMP, _, Some(ts)) =>
          val initial = data
            .find(
              _.approximateArrivalTimestamp.toEpochMilli() >= ts
                .toEpochMilli()
            )
            .getOrElse(data.last)

          val firstIndex = data.indexOf(initial)
          val lastIndex = Math.min(data.length - 1, firstIndex + maxRecords)
          val recs = data.slice(firstIndex, lastIndex)
          Valid((recs, recs.last.sequenceNumber))
        case (ShardIteratorType.AT_SEQUENCE_NUMBER, Some(seqNo), _) =>
          data.find(_.sequenceNumber == seqNo) match {
            case Some(initial) =>
              val firstIndex = data.indexOf(initial)
              val lastIndex = Math.min(data.length - 1, firstIndex + maxRecords)
              val recs = data.slice(firstIndex, lastIndex)

              Valid(
                (recs, recs.last.sequenceNumber)
              )
            case None =>
              ResourceNotFoundException(
                s"Unable to find record with provided SequenceNumber"
              ).invalidNel
          }

        case (ShardIteratorType.AFTER_SEQUENCE_NUMBER, Some(seqNo), _) =>
          data.find(_.sequenceNumber == seqNo) match {
            case Some(initial) =>
              val firstIndex =
                Math.min(data.indexOf(initial) + 1, data.length - 1)
              val lastIndex = Math.min(data.length - 1, firstIndex + maxRecords)
              val recs = data.slice(firstIndex, lastIndex)
              Valid(
                (recs, recs.last.sequenceNumber)
              )
            case None =>
              ResourceNotFoundException(
                s"Unable to find record with provided SequenceNumber"
              ).invalidNel
          }
      }
  }
}

object StartingPosition {
  implicit val startingPositionCirceEncoder: Encoder[StartingPosition] =
    Encoder.forProduct3("SequenceNumber", "Timestamp", "Type")(x =>
      (x.sequenceNumber, x.timestamp, x.`type`)
    )

  implicit val startingPositionCirceDecoder: Decoder[StartingPosition] = x =>
    for {
      sequenceNumber <- x.downField("SequenceNumber").as[Option[SequenceNumber]]
      timestamp <- x.downField("Timestamp").as[Option[Instant]]
      `type` <- x.downField("Type").as[ShardIteratorType]
    } yield StartingPosition(sequenceNumber, timestamp, `type`)
}

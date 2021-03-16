package kinesis.mock.models

import java.time.Instant

import io.circe._

final case class Consumer(
    consumerArn: String,
    consumerCreationTimestamp: Instant,
    consumerName: String,
    consumerStatus: ConsumerStatus,
    isProcessing: Boolean
)

object Consumer {
  def create(streamArn: String, consumerName: String): Consumer = {
    val consumerCreationTimestamp = Instant.now()
    Consumer(
      s"$streamArn/consumer/$consumerName:${consumerCreationTimestamp.getEpochSecond()}",
      consumerCreationTimestamp,
      consumerName,
      ConsumerStatus.CREATING,
      false
    )
  }
  implicit val consumerCirceEncoder: Encoder[Consumer] = Encoder.forProduct5(
    "ConsumerARN",
    "ConsumerCreationTimestamp",
    "ConsumerName",
    "ConsumerStatus",
    "IsProcessing"
  )(x =>
    (
      x.consumerArn,
      x.consumerCreationTimestamp,
      x.consumerName,
      x.consumerStatus,
      x.isProcessing
    )
  )

  implicit val consumerCirceDecoder: Decoder[Consumer] = { x =>
    for {
      consumerArn <- x.downField("ConsumerARN").as[String]
      consumerCreationTimestamp <- x
        .downField("ConsumerCreationTimestamp")
        .as[Instant]
      consumerName <- x.downField("ConsumerName").as[String]
      consumerStatus <- x.downField("ConsumerStatus").as[ConsumerStatus]
      isProcessing <- x.downField("IsProcessing").as[Boolean]
    } yield Consumer(
      consumerArn,
      consumerCreationTimestamp,
      consumerName,
      consumerStatus,
      isProcessing
    )
  }
}

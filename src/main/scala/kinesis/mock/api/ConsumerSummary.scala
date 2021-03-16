package kinesis.mock.api

import java.time.Instant

import io.circe._
import kinesis.mock.models.ConsumerStatus
import kinesis.mock.models.Consumer

final case class ConsumerSummary(
    consumerArn: String,
    consumerCreationTimestamp: Instant,
    consumerName: String,
    consumerStatus: ConsumerStatus
)

object ConsumerSummary {
  def fromConsumer(consumer: Consumer): ConsumerSummary = ConsumerSummary(
    consumer.consumerArn,
    consumer.consumerCreationTimestamp,
    consumer.consumerName,
    consumer.consumerStatus
  )
  implicit val consumerCirceEncoder: Encoder[ConsumerSummary] =
    Encoder.forProduct4(
      "ConsumerARN",
      "ConsumerCreationTimestamp",
      "ConsumerName",
      "ConsumerStatus"
    )(x =>
      (
        x.consumerArn,
        x.consumerCreationTimestamp,
        x.consumerName,
        x.consumerStatus
      )
    )

  implicit val consumerCirceDecoder: Decoder[ConsumerSummary] = { x =>
    for {
      consumerArn <- x.downField("ConsumerARN").as[String]
      consumerCreationTimestamp <- x
        .downField("ConsumerCreationTimestamp")
        .as[Instant]
      consumerName <- x.downField("ConsumerName").as[String]
      consumerStatus <- x.downField("ConsumerStatus").as[ConsumerStatus]
    } yield ConsumerSummary(
      consumerArn,
      consumerCreationTimestamp,
      consumerName,
      consumerStatus
    )
  }
}

package kinesis.mock.api

import io.circe._

final case class DescribeStreamConsumerResponse(consumerDescription: ConsumerSummary)

object DescribeStreamConsumerResponse {
  implicit val describeStreamConsumerResponseCirceEncoder
      : Encoder[DescribeStreamConsumerResponse] =
    Encoder.forProduct1("ConsumerDescription")(_.consumerDescription)

  implicit val describeStreamConsumerResponseCirceDecoder
      : Decoder[DescribeStreamConsumerResponse] = {
    _.downField("ConsumerDescription")
      .as[ConsumerSummary]
      .map(DescribeStreamConsumerResponse.apply)
  }
}

package hmda.api.http.filing.submissions

import java.time.Instant

import akka.NotUsed
import akka.actor.ActorSystem
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityRef}
import akka.http.scaladsl.server.Directives._
import akka.event.LoggingAdapter
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives.{
  complete,
  encodeResponse,
  fileUpload,
  handleRejections,
  onComplete,
  path,
  pathPrefix
}
import akka.http.scaladsl.server.Route
import akka.kafka.ProducerMessage.MultiResultPart
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Framing, Sink}
import akka.util.{ByteString, Timeout}
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.{
  cors,
  corsRejectionHandler
}
import com.typesafe.config.Config
import hmda.api.http.directives.HmdaTimeDirectives
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.auto._
import hmda.api.http.codec.filing.submission.SubmissionStatusCodec._
import hmda.api.http.codec.ErrorResponseCodec._
import hmda.util.http.FilingResponseUtils._
import hmda.api.http.model.ErrorResponse
import hmda.messages.submission.SubmissionCommands.GetSubmission
import hmda.model.filing.submission._
import hmda.persistence.submission.{SubmissionManager, SubmissionPersistence}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import hmda.messages.pubsub.KafkaTopics._
import hmda.messages.submission.SubmissionManagerCommands.{
  SubmissionManagerCommand,
  UpdateSubmissionStatus
}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait UploadHttpApi extends HmdaTimeDirectives {

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit val ec: ExecutionContext
  val log: LoggingAdapter
  val sharding: ClusterSharding
  implicit val timeout: Timeout
  val config: Config

  // institutions/<lei>/filings/<period>/submissions/<seqNr>
  val uploadHmdaFileRoute =
    path(Segment / "filings" / Segment / "submissions" / IntNumber) {
      (lei, period, seqNr) =>
        timedPost { uri =>
          val submissionId = SubmissionId(lei, period, seqNr)
          val uploadTimestamp = Instant.now.toEpochMilli

          val submissionManager =
            sharding.entityRefFor(
              SubmissionManager.typeKey,
              s"${SubmissionManager.name}-${submissionId.toString}")

          val submissionPersistence =
            sharding.entityRefFor(
              SubmissionPersistence.typeKey,
              s"${SubmissionPersistence.name}-${submissionId.toString}")

          val fSubmission
          : Future[Option[Submission]] = submissionPersistence ? (ref =>
            GetSubmission(ref))

          val fCheckSubmission = for {
            s <- fSubmission.mapTo[Option[Submission]]
          } yield s

          onComplete(fCheckSubmission) {
            case Success(result) =>
              result match {
                case Some(submission) =>
                  if (submission.status == Created) {
                    uploadFile(submissionManager,
                      uploadTopic,
                      uploadTimestamp,
                      submission,
                      uri)
                  } else {
                    val errorResponse = ErrorResponse(
                      404,
                      s"Submissidson ${submissionId.toString} not available for upload",
                      uri.path)
                    complete(
                      ToResponseMarshallable(
                        StatusCodes.NotFound -> errorResponse)
                    )
                  }
                case None =>
                  val errorResponse = ErrorResponse(
                    404,
                    s"Submissiasdfon ${submissionId.toString} not available for upload",
                    uri.path)
                  complete(
                    ToResponseMarshallable(
                      StatusCodes.NotFound -> errorResponse)
                  )
              }
            case Failure(error) =>
              failedResponse(StatusCodes.InternalServerError, uri, error)
          }
        }
    }

  def uploadRoutes: Route = {
    handleRejections(corsRejectionHandler) {
      cors() {
        encodeResponse {
          pathPrefix("institutions") {
            uploadHmdaFileRoute
          }
        }
      }
    }
  }

  private def uploadFile(submissionManager: EntityRef[SubmissionManagerCommand],
                         topic: String,
                         uploadTimeStamp: Long,
                         submission: Submission,
                         uri: Uri): Route = {
    val splitLines =
      Framing.delimiter(ByteString("\n"), 2048, allowTruncation = true)

    fileUpload("file") {
      case (_, byteSource) =>
        val modified = submission.copy(status = Uploading)
        submissionManager ! UpdateSubmissionStatus(modified)
        val fUploaded = byteSource
          .via(splitLines)
          .map(_.utf8String + "\n")
          .via(uploadProducer(topic, submission.id))
          .runWith(Sink.ignore)

        onComplete(fUploaded) {
          case Success(_) =>
            val modified = submission.copy(status = Uploaded)
            submissionManager ! UpdateSubmissionStatus(modified)
            complete(
              ToResponseMarshallable(
                StatusCodes.Accepted -> submission.copy(status = Uploaded)))
          case Failure(error) =>
            val failed = submission.copy(status = Failed)
            submissionManager ! UpdateSubmissionStatus(failed)
            log.error(error.getLocalizedMessage)
            val errorResponse =
              ErrorResponse(400, "Invalid file format", uri.path)
            complete(
              ToResponseMarshallable(StatusCodes.BadRequest -> errorResponse))
        }

      case _ =>
        val errorResponse = ErrorResponse(400, "Invalid file format", uri.path)
        complete(
          ToResponseMarshallable(StatusCodes.BadRequest -> errorResponse))
    }
  }

  private def uploadProducer(
                              topic: String,
                              submissionId: SubmissionId): Flow[String, String, NotUsed] = {

    val kafkaHosts = config.getString("kafka.hosts")
    val kafkaConfig = system.settings.config.getConfig("akka.kafka.producer")
    val producerSettings =
      ProducerSettings(kafkaConfig, new StringSerializer, new StringSerializer)
        .withBootstrapServers(kafkaHosts)

    Flow[String]
      .map { value =>
        ProducerMessage.Message(
          new ProducerRecord(topic, submissionId.toString, value),
          value
        )
      }
      .via(Producer.flexiFlow(producerSettings))
      .map {
        case ProducerMessage.Result(_, message) =>
          val record = message.record
          record.value()
        case ProducerMessage.MultiResult(parts, _) =>
          parts
            .map {
              case MultiResultPart(_, record) =>
                record.value()
            }
            .mkString(",")
        case ProducerMessage.PassThroughResult(passThrough) =>
          passThrough
      }
  }

}
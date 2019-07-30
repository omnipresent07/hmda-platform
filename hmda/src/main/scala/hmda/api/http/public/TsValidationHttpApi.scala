package hmda.api.http.public

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.headers.RawHeader
import hmda.parser.filing.ts.TsCsvParser
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import hmda.api.http.model.public.TsValidateRequest
import hmda.api.http.codec.filing.TsCodec._
import hmda.api.http.directives.HmdaTimeDirectives
import io.circe.generic.auto._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import hmda.model.filing.ts.TransmittalSheet
import hmda.model.validation.TsValidationError
import hmda.validation.HmdaValidation
import hmda.validation.context.ValidationContext
import hmda.validation.engine._

import scala.concurrent.ExecutionContext

trait TsValidationHttpApi
    extends HmdaTimeDirectives
    with FilingValidationHttpApi {

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  val log: LoggingAdapter
  implicit val ec: ExecutionContext
  implicit val timeout: Timeout

  //ts/parse
  val parseTsRoute =
    path("parse") {
      timedPost { _ =>
        respondWithHeader(RawHeader("Cache-Control", "no-cache")) {
          entity(as[TsValidateRequest]) { req =>
            TsCsvParser(req.ts) match {
              case Right(ts) => complete(ToResponseMarshallable(ts))
              case Left(errors) =>
                completeWithParsingErrors(errors)
            }
          }
        }
      } ~
        timedOptions { _ =>
          complete("OPTIONS")
        }
    }

  //ts/validate/<year>
  val validateTsRoute =
    path("validate" / IntNumber) { year =>
      parameters('check.as[String] ? "all") { checkType =>
        timedPost { _ =>
          respondWithHeader(RawHeader("Cache-Control", "no-cache")) {
            entity(as[TsValidateRequest]) { req =>
              TsCsvParser(req.ts) match {
                case Right(ts) =>
                  validate(ts, checkType, ValidationContext(None), year)
                case Left(errors) =>
                  completeWithParsingErrors(errors)
              }
            }
          }
        }
      }
    }

  private def validate(ts: TransmittalSheet,
                       chekType: String,
                       ctx: ValidationContext,
                       year: Int): Route = {
    val ctx = ValidationContext(filingYear = Some(year))
    val validationEngine = selectTsEngine(year)
    import validationEngine._
    val validation: HmdaValidation[TransmittalSheet] = chekType match {
      case "all" => checkAll(ts, ts.LEI, ctx, TsValidationError)
      case "syntactical" =>
        checkSyntactical(ts, ts.LEI, ctx, TsValidationError)
      case "validity" =>
        checkValidity(ts, ts.LEI, TsValidationError)
    }

    val maybeErrors = validation.leftMap(xs => xs.toList).toEither

    maybeErrors match {
      case Right(t) => complete(t)
      case Left(errors) =>
        complete(ToResponseMarshallable(aggregateErrors(errors, year.toString)))
    }
  }

  def tsRoutes: Route = {
    handleRejections(corsRejectionHandler) {
      cors() {
        encodeResponse {
          pathPrefix("ts") {
            parseTsRoute ~ validateTsRoute
          }
        }
      }
    }
  }

}

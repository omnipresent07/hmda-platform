package hmda.dataBrowser.api

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.{ HttpEntity, StatusCodes, Uri }
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.stream.ActorMaterializer
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import hmda.dataBrowser.Settings
import hmda.dataBrowser.api.DataBrowserDirectives._
import hmda.dataBrowser.models.HealthCheckStatus.Up
import hmda.dataBrowser.models._
import hmda.dataBrowser.repositories._
import hmda.dataBrowser.services._
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.{ ClientOptions, RedisClient }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

import scala.util.{ Failure, Success }

trait DataBrowserHttpApi extends Settings {

  val Csv          = "csv"
  val Pipe         = "pipe"
  val Aggregations = "aggregations"
  val log: LoggingAdapter
  implicit val materializer: ActorMaterializer

  val databaseConfig = DatabaseConfig.forConfig[JdbcProfile]("databrowser_db")
  val repository =
    new PostgresModifiedLarRepository(database.tableName, databaseConfig)

  // We make the creation of the Redis client effectful because it can fail and we would like to operate
  // the service even if the cache is down (we provide fallbacks in case we receive connection errors)
  val redisClientTask: Task[RedisAsyncCommands[String, String]] = {
    val client = RedisClient.create(redis.url)
    Task.eval {
      client.setOptions(
        ClientOptions
          .builder()
          .autoReconnect(true)
          .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
          .cancelCommandsOnReconnectFailure(true)
          .build()
      )

      client
        .connect()
        .async()
    }.memoizeOnSuccess
    // we memoizeOnSuccess because if we manage to create the client, we do not want to recompute it because the
    // client creation process is expensive and the client is able to recover internally when Redis comes back
  }

  val cache =
    new RedisModifiedLarAggregateCache(redisClientTask, redis.ttl)

  val query: QueryService =
    new ModifiedLarBrowserService(repository, cache)

  val fileCache = new S3FileService

  val healthCheck: HealthCheckService =
    new HealthCheckService(repository, cache, fileCache)

  def serveData(queries: List[QueryField], delimiter: Delimiter, errorMessage: String): Route =
    onComplete(obtainDataSource(fileCache, query)(queries, delimiter).runToFuture) {
      case Failure(ex) =>
        log.error(ex, errorMessage)
        complete(StatusCodes.InternalServerError)

      case Success(Left(byteSource)) =>
        complete(
          HttpEntity(`text/plain(UTF-8)`, byteSource)
        )

      case Success(Right(url)) =>
        redirect(Uri(url), StatusCodes.MovedPermanently)
    }

  val dataBrowserRoutes: Route =
    encodeResponse {
      pathPrefix("view") {
        pathPrefix("count") {
          extractCountFields { countFields =>
            log.info("Counts: " + countFields)
            complete(
              query
                .fetchAggregate(countFields)
                .map(aggs => AggregationResponse(Parameters.fromBrowserFields(countFields), aggs))
                .runToFuture
            )
          }
        } ~
          pathPrefix("nationwide") {
            extractFieldsForRawQueries { queryFields =>
              // GET /view/nationwide/csv
              (path(Csv) & get) {
                extractNationwideMandatoryYears { mandatoryFields =>
                  //remove filters that have all options selected
                  val allFields = (queryFields ++ mandatoryFields).filterNot { eachQueryField =>
                    eachQueryField.isAllSelected
                  }
                  log.info("Nationwide [CSV]: " + allFields)
                  contentDispositionHeader(allFields, Commas) {
                    serveData(allFields, Commas, s"Failed to perform nationwide CSV query with the following queries: $allFields")
                  }
                }
              } ~
                // GET /view/nationwide/pipe
                (path(Pipe) & get) {
                  extractNationwideMandatoryYears { mandatoryFields =>
                    //remove filters that have all options selected
                    val allFields = (queryFields ++ mandatoryFields).filterNot { eachQueryField =>
                      eachQueryField.isAllSelected
                    }
                    log.info("Nationwide [Pipe]: " + allFields)
                    contentDispositionHeader(allFields, Pipes) {
                      serveData(allFields, Pipes, s"Failed to perform nationwide PSV query with the following queries: $allFields")
                    }
                  }

                }
            } ~
              // GET /view/nationwide/aggregations
              (path(Aggregations) & get) {
                extractFieldsForAggregation { queryFields =>
                  val allFields = queryFields
                  log.info("Nationwide [Aggregations]: " + allFields)
                  complete(
                    query
                      .fetchAggregate(allFields)
                      .map(aggs => AggregationResponse(Parameters.fromBrowserFields(allFields), aggs))
                      .runToFuture
                  )
                }
              }
          } ~
          // GET /view/aggregations
          (path(Aggregations) & get) {
            extractYearsAndMsaAndStateAndCountyAndLEIBrowserFields { mandatoryFields =>
               extractFieldsForAggregation { remainingQueryFields =>
                 val allFields = mandatoryFields ++ remainingQueryFields
                log.info("Aggregations: " + allFields)
                complete(
                  query
                    .fetchAggregate(allFields)
                    .map(aggs => AggregationResponse(Parameters.fromBrowserFields(allFields), aggs))
                    .runToFuture
                )
              }
            }
          } ~
          // GET /view/csv
          (path(Csv) & get) {
            extractYearsAndMsaAndStateAndCountyAndLEIBrowserFields { mandatoryFields =>
              extractFieldsForRawQueries { remainingQueryFields =>
                val allFields = mandatoryFields ++ remainingQueryFields
                log.info("CSV: " + allFields)
                contentDispositionHeader(allFields, Commas) {
                  serveData(allFields, Commas, s"Failed to fetch data for /view/csv with the following queries: $allFields")
                }
              }
            }
          } ~
          // GET /view/pipe
          (path(Pipe) & get) {
            extractYearsAndMsaAndStateAndCountyAndLEIBrowserFields { mandatoryFields =>
              extractFieldsForRawQueries { remainingQueryFields =>
                val allFields = mandatoryFields ++ remainingQueryFields
                log.info("PIPE: " + allFields)
                contentDispositionHeader(allFields, Pipes) {
                  serveData(allFields, Pipes, s"Failed to fetch data for /view/pipe with the following queries: $allFields")
                }
              }
            }
          }
      } ~
        pathPrefix("health") {
          onComplete(healthCheck.healthCheckStatus.runToFuture) {
            case Success(hs @ HealthCheckResponse(Up, Up, Up)) =>
              complete(StatusCodes.OK)

            case Success(hs) =>
              complete(StatusCodes.ServiceUnavailable)

            case Failure(ex) =>
              log.error(ex, "Failed to perform a health check")
              complete(StatusCodes.InternalServerError)
          }
        }

    }
}
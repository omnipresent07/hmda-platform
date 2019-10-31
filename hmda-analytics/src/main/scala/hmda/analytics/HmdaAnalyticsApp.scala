package hmda.analytics

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.{ ConsumerSettings, Subscriptions }
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.util.{ ByteString, Timeout }
import com.typesafe.config.ConfigFactory
import hmda.query.ts.TransmittalSheetConverter
import hmda.analytics.query.{
  LarComponent,
  LarComponent2018,
  LarConverter,
  LarConverter2018,
  SubmissionHistoryComponent,
  TransmittalSheetComponent
}
import hmda.model.filing.lar.LoanApplicationRegister
import hmda.model.filing.submission.SubmissionId
import hmda.model.filing.ts.TransmittalSheet
import hmda.parser.filing.lar.LarCsvParser
import hmda.parser.filing.ts.TsCsvParser
import hmda.publication.KafkaUtils.kafkaHosts
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import hmda.messages.pubsub.HmdaTopics
import hmda.messages.pubsub.HmdaGroups
import hmda.query.DbConfiguration.dbConfig
import hmda.query.HmdaQuery.{ readRawData, readSubmission }
import hmda.util.BankFilterUtils._
import hmda.util.streams.FlowUtils.framing
import hmda.utils.YearUtils

import scala.concurrent.Future
import scala.concurrent.duration._

object HmdaAnalyticsApp extends App with TransmittalSheetComponent with LarComponent2018 with LarComponent with SubmissionHistoryComponent {

  val log = LoggerFactory.getLogger("hmda")

  log.info("""
             | _    _ __  __ _____                                 _       _   _
             || |  | |  \/  |  __ \   /\         /\               | |     | | (_)
             || |__| | \  / | |  | | /  \       /  \   _ __   __ _| |_   _| |_ _  ___ ___
             ||  __  | |\/| | |  | |/ /\ \     / /\ \ | '_ \ / _` | | | | | __| |/ __/ __|
             || |  | | |  | | |__| / ____ \   / ____ \| | | | (_| | | |_| | |_| | (__\__ \
             ||_|  |_|_|  |_|_____/_/    \_\ /_/    \_\_| |_|\__,_|_|\__, |\__|_|\___|jmo/
             |                                                        __/ |
             |                                                       |___/
    """.stripMargin)

  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec           = system.dispatcher

  implicit val timeout = Timeout(5.seconds)

  val kafkaConfig = system.settings.config.getConfig("akka.kafka.consumer")
  val config      = ConfigFactory.load()
  val bankFilter  = config.getConfig("filter")
  val bankFilterList =
    bankFilter.getString("bank-filter-list").toUpperCase.split(",")
  val parallelism = config.getInt("hmda.analytics.parallelism")

  /**
    * Note: hmda-analytics microservice reads the JDBC_URL env var from inst-postgres-credentials secret.
    * In beta namespace this environment variable has currentSchema=hmda_beta_user appended to it to change the schema
    * to BETA
    */
  val tsTableName2018  = config.getString("hmda.analytics.2018.tsTableName")
  val larTableName2018 = config.getString("hmda.analytics.2018.larTableName")
  //submission_history table remains same regardless of the year. There is a sign_date column and submission_id column which would show which year the filing was for
  val histTableName    = config.getString("hmda.analytics.2018.historyTableName")
  val tsTableName2019  = config.getString("hmda.analytics.2019.tsTableName")
  val larTableName2019 = config.getString("hmda.analytics.2019.larTableName")
  val tsTableName2020  = config.getString("hmda.analytics.2020.tsTableName")
  val larTableName2020 = config.getString("hmda.analytics.2020.larTableName")

  val transmittalSheetRepository2018 = new TransmittalSheetRepository(dbConfig, tsTableName2018)
  val transmittalSheetRepository2019 = new TransmittalSheetRepository(dbConfig, tsTableName2019)
  val transmittalSheetRepository2020 = new TransmittalSheetRepository(dbConfig, tsTableName2020)
  val larRepository2018              = new LarRepository2018(dbConfig, larTableName2018)
  val larRepository2019              = new LarRepository(dbConfig, larTableName2019)
  val larRepository2020              = new LarRepository(dbConfig, larTableName2020)
  val submissionHistoryRepository    = new SubmissionHistoryRepository(dbConfig, histTableName)

  val consumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(kafkaConfig, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(kafkaHosts)
      .withGroupId(HmdaGroups.analyticsGroup)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  Consumer
    .committableSource(consumerSettings, Subscriptions.topics(HmdaTopics.signTopic, HmdaTopics.analyticsTopic))
    .mapAsync(parallelism) { msg =>
      log.info(s"Processing: $msg")
      processData(msg.record.value()).map(_ => msg.committableOffset)
    }
    .mapAsync(parallelism * 2)(offset => offset.commitScaladsl())
    .toMat(Sink.seq)(Keep.both)
    .mapMaterializedValue(DrainingControl.apply)
    .run()

  def processData(msg: String): Future[Done] =
    Source
      .single(msg)
      .map(msg => SubmissionId(msg))
      .filter(institution => filterBankWithLogging(institution.lei, bankFilterList))
      .mapAsync(1) { id =>
        log.info(s"Adding data for  $id")
        addTs(id)
      }
      .toMat(Sink.ignore)(Keep.right)
      .run()
  private def addTs(submissionId: SubmissionId): Future[Done] = {
    var submissionIdVar = None: Option[String]
    submissionIdVar = Some(submissionId.toString)

    def signDate: Future[Option[Long]] =
      readSubmission(submissionId)
        .map(l => l.submission.end)
        .runWith(Sink.lastOption)

    def deleteTsRow: Future[Done] =
      readRawData(submissionId)
        .map(l => l.data)
        .take(1)
        .map(s => TsCsvParser(s, fromCassandra = true))
        .map(_.getOrElse(TransmittalSheet()))
        .filter(t => t.LEI != "" && t.institutionName != "")
        .map(ts => TransmittalSheetConverter(ts, submissionIdVar))
        .mapAsync(1) { ts =>
          for {
            delete <- submissionId.period.split("-") match {
                       case Array("2018") => transmittalSheetRepository2018.deleteByLei(ts.lei)
                       case Array("2019") => transmittalSheetRepository2019.deleteByLei(ts.lei)
                       case Array(_, "2020") =>
                         transmittalSheetRepository2020.deleteByLeiAndQuarter(lei = ts.lei)
                       case _ => transmittalSheetRepository2020.deleteByLei(ts.lei)
                     }
          } yield delete
        }
        .runWith(Sink.ignore)

    def insertSubmissionHistory: Future[Done] =
      readRawData(submissionId)
        .map(l => l.data)
        .map(ByteString(_))
        .via(framing("\n"))
        .map(_.utf8String)
        .map(_.trim)
        .take(1)
        .map(s => TsCsvParser(s, fromCassandra = true))
        .map(_.getOrElse(TransmittalSheet()))
        .filter(t => t.LEI != "" && t.institutionName != "")
        .map(ts => TransmittalSheetConverter(ts, submissionIdVar))
        .mapAsync(1) { ts =>
          for {
            signdate          <- signDate
            submissionHistory <- submissionHistoryRepository.insert(ts.lei, submissionId, signdate)
          } yield submissionHistory
        }
        .runWith(Sink.ignore)

    def insertTsRow: Future[Done] =
      readRawData(submissionId)
        .map(l => l.data)
        .map(ByteString(_))
        .via(framing("\n"))
        .map(_.utf8String)
        .map(_.trim)
        .take(1)
        .map(s => TsCsvParser(s, fromCassandra = true))
        .map(_.getOrElse(TransmittalSheet()))
        .filter(t => t.LEI != "" && t.institutionName != "")
        .map(ts => TransmittalSheetConverter(ts, submissionIdVar))
        .mapAsync(1) { ts =>
          for {
            insertorupdate <- submissionId.period.split("-") match {
                               case Array("2018") => transmittalSheetRepository2018.insert(ts)
                               case Array("2019") => transmittalSheetRepository2019.insert(ts)
                               case Array(_, "2020") =>
                                 transmittalSheetRepository2020.insert(ts.copy(isQuarterly = Some(true)))
                               case _ => transmittalSheetRepository2020.insert(ts)
                             }

          } yield insertorupdate
        }
        .runWith(Sink.ignore)

    def deleteLarRows: Future[Done] =
      readRawData(submissionId)
        .map(l => l.data)
        .map(ByteString(_))
        .via(framing("\n"))
        .map(_.utf8String)
        .map(_.trim)
        .drop(1)
        .take(1)
        .map(s => LarCsvParser(s, true))
        .map(_.getOrElse(LoanApplicationRegister()))
        .filter(lar => lar.larIdentifier.LEI != "")
        .mapAsync(1) { lar =>
          for {
            delete <- submissionId.period.split("-") match {
                       case Array("2018")    => larRepository2018.deleteByLei(lar.larIdentifier.LEI)
                       case Array("2019")    => larRepository2019.deleteByLei(lar.larIdentifier.LEI)
                       case Array(_, "2020") => larRepository2020.deletebyLeiAndQuarter(lar.larIdentifier.LEI)
                       case _                => larRepository2020.deleteByLei(lar.larIdentifier.LEI)
                     }
          } yield delete
        }
        .runWith(Sink.ignore)

    def insertLarRows: Future[Done] =
      readRawData(submissionId)
        .map(l => l.data)
        .map(ByteString(_))
        .via(framing("\n"))
        .map(_.utf8String)
        .map(_.trim)
        .drop(1)
        .map(s => LarCsvParser(s, true))
        .map(_.getOrElse(LoanApplicationRegister()))
        .filter(lar => lar.larIdentifier.LEI != "")
        .mapAsync(1) { lar =>
          for {
            insertorupdate <- submissionId.period.split("-") match {
                               case Array("2018") => larRepository2018.insert(LarConverter2018(lar))
                               case Array("2019") =>
                                 larRepository2019.insert(
                                   LarConverter(lar = lar)
                                 )
                               case Array(_, "2020") =>
                                 larRepository2020.insertQuarterly(
                                   LarConverter(lar = lar, isQuarterly = true)
                                 )
                               case _ => larRepository2020.insert(LarConverter(lar))
                             }
          } yield insertorupdate
        }
        .runWith(Sink.ignore)

    def result =
      for {
        _ <- deleteTsRow
        _ = log.info(s"Deleting data from TS for  $submissionId")

        _ <- insertTsRow
        _ = log.info(s"Adding data into TS for  $submissionId")

        _ <- deleteLarRows
        _ = log.info(s"Done deleting data from LAR for  $submissionId")

        _ <- insertLarRows
        _ = log.info(s"Done inserting data into LAR for  $submissionId")

        _   <- signDate
        res <- insertSubmissionHistory
      } yield res
    result.recover {
      case t: Throwable =>
        log.error("Error happened in inserting: ", t)
        throw t
    }

  }

}

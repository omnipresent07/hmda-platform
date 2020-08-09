package com.hmda.reports.processing

import akka.Done
import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern.pipe
import akka.stream._
import akka.stream.alpakka.s3.S3Settings
import akka.stream.scaladsl._
import com.hmda.reports.model._
import com.hmda.reports.processing.DisclosureProcessingWorker.ProcessDisclosureKafkaRecord
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.{SparkSession, _}

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class DisclosureProcessingSupervisor(spark: SparkSession, s3Settings: S3Settings) extends Actor with ActorLogging {

  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case e: Exception =>
        log.error(e, "DisclosureProcessingSupervisor received an exception from child")
        Restart
    }

  override def receive: Receive = {
    case msg: ProcessDisclosureKafkaRecord =>
      val originalSender = sender()
      val worker = context.actorOf(DisclosureProcessingWorker.props(spark, s3Settings))
      worker.tell(msg, originalSender)
  }
}

object DisclosureProcessingSupervisor {
  def props(spark: SparkSession, s3Settings: S3Settings): Props = Props(new DisclosureProcessingSupervisor(spark, s3Settings))
}

class DisclosureProcessingWorker(spark: SparkSession, s3Settings: S3Settings) extends Actor with ActorLogging {
  import DisclosureProcessingWorker._

  implicit val mat: ActorMaterializer = ActorMaterializer()(context.system)
  implicit val ec: ExecutionContext   = context.dispatcher

  override def receive: Receive = {
    case ProcessDisclosureKafkaRecord(lei, lookupMap, jdbcUrl, bucket, year) =>
      val originalSender = sender()
      log.info(s"Beginning process for $lei")
      processDisclosureKafkaRecord(lei, spark, lookupMap, jdbcUrl, bucket, year, s3Settings).map(_ => Finished)
        .pipeTo(originalSender)
        .onComplete {
          case Success(_) =>
            log.info(s"Finished process for $lei")
            context.stop(self)

          case Failure(exception) =>
            log.error(exception, s"Failed to complete process for $lei")
            context.stop(self)
        }
  }
}

object DisclosureProcessingWorker {
  case class ProcessDisclosureKafkaRecord(
                                           lei: String,
                                           lookupMap: Map[(Int, Int), StateMapping],
                                           jdbcUrl: String,
                                           bucket: String,
                                           year: String
                                         )
  case object Finished

  def props(sparkSession: SparkSession, s3Settings: S3Settings): Props =
    Props(new DisclosureProcessingWorker(sparkSession, s3Settings))

  def processDisclosureKafkaRecord(
                                    lei: String,
                                    spark: SparkSession,
                                    lookupMap: Map[(Int, Int), StateMapping],
                                    jdbcUrl: String,
                                    bucket: String,
                                    year: String,
                                    s3Settings: S3Settings
                                  )(implicit mat: ActorMaterializer, ec: ExecutionContext): Future[Unit] = {
    import spark.implicits._

    def jsonFormationTable1(msaMd: Msa, input: List[Data], leiDetails: Institution): OutDisclosure1 = {
      val dateFormat = new java.text.SimpleDateFormat("MM/dd/yyyy hh:mm aa")

      val tracts = input
        .groupBy(d => d.msa_md)
        .flatMap {
          case (msa, datasByMsa) =>
            val tracts: List[Tract] = datasByMsa
              .groupBy(_.tract)
              .map {
                case (tract, datasByTract) =>
                  val dispositions: List[Disposition] = datasByTract
                    .groupBy(d => d.title)
                    .map {
                      case (title, datasByTitle) =>
                        val listInfo: List[Info] = datasByTitle.map(d => Info(d.dispositionName, d.count, d.loan_amount))
                        Disposition(title.split("-")(0).trim, listInfo, title)
                    }
                    .toList
                    .sorted
                  val stateCode      = Try(tract.take(2).toInt).getOrElse(-1)
                  val countyCode     = Try(tract.slice(2, 5).toInt).getOrElse(-1)
                  val remainingTract = tract.drop(5)
                  val stateMapping =
                    lookupMap.getOrElse((stateCode, countyCode), StateMapping())
                  Tract(stateMapping.county + "/" + stateMapping.stateName + "/" + remainingTract, dispositions)
                //                  Tract("", dispositions)
              }
              .toList
            tracts
        }
        .toList
      OutDisclosure1(
        leiDetails.lei,
        leiDetails.institutionName,
        "1",
        "Disclosure",
        "Disposition of loan applications, by location of property and type of loan",
        year.toInt,
        dateFormat.format(new java.util.Date()),
        msaMd,
        tracts
      )
    }

    def jsonFormationTable2(msaMd: Msa, input: List[Data], leiDetails: Institution): OutDisclosure2 = {
      val dateFormat = new java.text.SimpleDateFormat("MM/dd/yyyy hh:mm aa")

      val tracts = input
        .groupBy(d => d.msa_md)
        .flatMap {
          case (msa, datasByMsa) =>
            val tracts: List[Tract2] = datasByMsa
              .groupBy(_.tract)
              .map {
                case (tract, datasByTract) =>
                  val dispositions: List[Disposition] = datasByTract
                    .groupBy(d => d.title)
                    .map {
                      case (title, datasByTitle) =>
                        val listInfo: List[Info] = datasByTitle.map(d => Info(d.dispositionName, d.count, d.loan_amount))
                        Disposition(title.split("-")(0).trim, listInfo, title)
                    }
                    .toList
                    .sorted
                  val stateCode      = Try(tract.take(2).toInt).getOrElse(-1)
                  val countyCode     = Try(tract.slice(2, 5).toInt).getOrElse(-1)
                  val remainingTract = tract.drop(5)
                  val stateMapping =
                    lookupMap.getOrElse((stateCode, countyCode), StateMapping())
                  Tract2(stateMapping.county + "/" + stateMapping.stateName + "/" + remainingTract, dispositions(0).values)
                //                  Tract2("", dispositions(0).values)
              }
              .toList
            tracts
        }
        .toList

      // Disclosure2(msaMd, tracts)
      OutDisclosure2(
        leiDetails.lei,
        leiDetails.institutionName,
        "2",
        "Disclosure",
        "Loans purchased, by location of property and type of loan",
        year.toInt,
        dateFormat.format(new java.util.Date()),
        msaMd,
        tracts
      )
    }

    def persistJson(input: List[OutDisclosure1]): Future[Done] =
      Source(input)
        .mapAsyncUnordered(10) { input =>
          val data: String = input.asJson.noSpaces
          BaseProcessing.persistSingleFile(
            s"$bucket/reports/disclosure/$year/$lei/${input.msa.id}/1.json",
            data,
            "cfpb-hmda-public",
            s3Settings
          )(mat, ec)
        }
        .runWith(Sink.ignore)

    def persistJson2(input: List[OutDisclosure2]): Future[Done] =
      Source(input)
        .mapAsyncUnordered(10) { input =>
          val data: String = input.asJson.noSpaces
          BaseProcessing.persistSingleFile(
            s"$bucket/reports/disclosure/$year/$lei/${input.msa.id}/2.json",
            data,
            "cfpb-hmda-public",
            s3Settings
          )(mat, ec)
        }
        .runWith(Sink.ignore)

    def leiDetails: Institution =
      spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", jdbcUrl)
        .option(
          "dbtable",
          s"(select lei, respondent_name as institutionName from institutions2018prod where lei = '$lei' and hmda_filer = true and lei = '549300R9XZ3VYIG5O043') as institutions2018"
        )
        .load()
        .as[Institution]
        .collect()
        .head

    def cachedRecordsDf: DataFrame =
      spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", jdbcUrl)
        .option("dbtable", s"(select * from modifiedlar2018prod where lei = '$lei' and filing_year = $year and lei = '549300R9XZ3VYIG5O043') as mlar")
        .load()
        .cache()

    def disclosuresTable1: List[OutDisclosure1] =
      BaseProcessing
        .outputCollectionTable1Disclosure(cachedRecordsDf, spark)
        .groupBy(d => d.msa_md)
        .map {
          case (key, values) =>
            val msaMd = Msa(key.toString, values.head.msa_md_name, "", "")
            jsonFormationTable1(msaMd, values, leiDetails)
        }
        .toList

    def disclosuresTable2: List[OutDisclosure2] =
      BaseProcessing
        .outputCollectionTable2Disclosure(cachedRecordsDf, spark)
        .groupBy(d => d.msa_md)
        .map {
          case (key, values) =>
            val msaMd = Msa(key.toString, values.head.msa_md_name, "", "")
            jsonFormationTable2(msaMd, values, leiDetails)
        }
        .toList

    val result = for {
//      _ <- persistJson(disclosuresTable1)
      _ <- persistJson2(disclosuresTable2)
    } yield ()

    result.onComplete {
      case Success(_) => println(s"Finished processing LEI: $lei")
      case Failure(exception) =>
        println(s"Exception happened when processing LEI $lei" + exception.getMessage)
        println("Printing stacktrace")
        exception.printStackTrace()
    }

    result
  }
}